<?php
/**
 * @copyright Copyright (c) 2019 Thomas Pulzer <t.pulzer@kniel.de>
 *
 * @author Thomas Pulzer <t.pulzer@kniel.de>
 *
 * @license GNU AGPL version 3 or any later version
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
namespace DB4D;

/**
 * Handle connections to a 4th Dimension database.
 * This class connects to and disconnects from the sql engine of an
 * 4th Dimension database server. It can also send 'raw' sql queries
 * as well as prepare statements for the DB4DStatement class.
 * @package DB4D
 */
class DB4DDriver {
	/**
	 * Fetch the result as numeric array.
	 */
	const FETCH_NUM = 0xA0;
	/**
	 * Fetch the result as associative array.
	 */
	const FETCH_ASSOC = 0xA1;
	/**
	 * Fetch the result as numeric-associative array.
	 */
	const FETCH_BOTH = 0xA2;
	/**
	 * The connection object.
	 * @access private
	 * @var resource
	 */
	private $connection;
	/**
	 * The current id of the command.
	 * @var int
	 */
	public $commandID;
	
	/**
	 * Create a DB4DDriver object and connect to the database.
	 * @param String $host The hostname or ip address of the 4D server.
	 * @param int $port The ip port where the 4D sql engine is listening.
	 * @param String $user The username used to connect to the database.
	 * @param String $pass The database user's password.
	 * @throws DB4DException If a connection could not be established.
	 */
	public function __construct($host, $port, $user, $pass) {

		$this->commandID=1;
		$this->connection = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
		if ($this->connection === false) {
			throw new DB4DException(socket_strerror(socket_last_error()), DB4DException::SOC_CREATION_ERROR);
		}
		if(!filter_var($host, FILTER_VALIDATE_IP)) {$host = gethostbyname($host);}
		$result = socket_connect($this->connection, $host, $port);
		if ($result === false) {
            socket_close($this->connection);
			throw new DB4DException(socket_strerror(socket_last_error()), DB4DException::SOC_CONNECTION_ERROR);
		}

		$in = str_pad($this->commandID, 3, "0", STR_PAD_LEFT)." LOGIN\r\nUSER-NAME-BASE64 : ".base64_encode($user)."\r\nUSER-PASSWORD-BASE64 : ".base64_encode($pass)."\r\nREPLY-WITH-BASE64-TEXT : N\r\nPROTOCOL-VERSION : 0.1a\r\n\r\n";
		$out = "";
		socket_write($this->connection, $in, strlen($in));

		$connectionState="";
		$errorCode="";
		$errorDescription="";
		while(($char=socket_read($this->connection, 1))!="") {
			$out .=$char;
			if(strpos($out, "\r\n")!==false) {
				if(strcmp($out, "\r\n")==0) {break;};
				if(strpos($out, ":")===false) {
					$connectionState = explode(" ", $out)[1];
					$out="";
					continue;
				}
				$aTmp = explode(":", $out);
				switch($aTmp[0]) {
					case("Error-Code"):
						$errorCode=$aTmp[1];
						$out="";
						break;
					case("Error-Description"):
						$errorDescription=$aTmp[1];
						$out="";
						break;
					default:
						$out="";
						break;
				}
			}
		}
		if(strpos($connectionState, "ERROR")!==false) {
			throw new DB4DException($errorDescription, $errorCode);
		}
		$this->commandID=$this->commandID+2;
	}

	/**
	 * Destroy a DB4DDriver object.
	 * Close the database connection and destroy the DB4D object.
	 */
	public function __destruct() {
		if(get_resource_type($this->connection)=="Socket") {$this->close();}
	}

    /**
     * Set database to transaction mode.
     * @throws DB4DException
     */
	public function beginTransaction() {
		$this->query("START");
	}
	
	/**
	 * Close the connection.
	 */
	public function close() {
		socket_shutdown($this->connection);
		socket_close($this->connection);
	}

    /**
     * Commit a transaction.
     * Write all changes made during the ongoing transaction.
     * @throws DB4DException
     */
	public function commit() {
		$this->query("COMMIT");
	}
	
	/**
	 * Prepare a statement for executing.
	 * Prepare a statement to be parsed and executed .
	 * @param String $query A SQL query. May contain question marks as placeholder for parameters injected at execution time.
	 * @return DB4DStatement A new DB4DStatement with the prepared query.
	 */
	public function prepare($query) {
		$preparedQuery = str_pad($this->commandID, 3, "0", STR_PAD_LEFT)." EXECUTE-STATEMENT\r\nSTATEMENT : ";
		$preparedQuery .= $query;
		$preparedQuery .= "\r\nOUTPUT-MODE : RELEASE\r\nFIRST-PAGE-SIZE : 1\r\n\r\n";
		$this->commandID = $this->commandID+4;
		return new DB4DStatement($this->connection, $preparedQuery, $this->commandID-4);
	}
	
	/**
	 * Query the database.
	 * Executes a 'raw' SQL query.
	 * @param String $sql A SQL query.
	 * @return mixed DB4DStatement|FALSE on a SELECT query. The number of rows affected on an INSERT, UPDATE or DELETE query
	 * @throws DB4DException
	 */
	public function query($sql) {
		$resultSet = new DB4DStatement($this->connection);
		$in = str_pad($this->commandID, 3, "0", STR_PAD_LEFT)." EXECUTE-STATEMENT\r\nSTATEMENT : ";
		$in .= $sql;
		$in .= "\r\nOUTPUT-MODE : RELEASE\r\nFIRST-PAGE-SIZE : 1\r\n\r\n";
		socket_write($this->connection, $in, strlen($in));

		$this->commandID=$this->commandID+2;
		$out = "";
		while(socket_last_error($this->connection)==0) {
			$char = socket_read($this->connection, 1);
			$out .=$char;
			if(strpos($out, "\r\n")===false) {continue;}
			if($resultSet->readStatementHeader($out)) {$out="";continue;}
			if($resultSet->errorOccured()) {
				throw new DB4DException("There was an error with the 4D statement: {$resultSet->getErrorCode()} .",DB4DException::STMT_RETURN_ERROR);
			}
			if($resultSet->resultType=="Update-Count \r\n") {
				return $resultSet->rowCount;
			}
			if($resultSet->rowCount==0) {return false;}
			if($resultSet->readStatementResult()) {break;}
			else {break;}
		}
		unset($out);
		
		$in = str_replace(str_pad($this->commandID, 3, "0", STR_PAD_LEFT)." EXECUTE-STATEMENT", str_pad($this->commandID+2, 3, "0", STR_PAD_LEFT)." EXECUTE-STATEMENT", $in);
		$in = str_replace("FIRST-PAGE-SIZE : 1", "FIRST-PAGE-SIZE : {$resultSet->rowCount}", $in);
		socket_write($this->connection, $in, strlen($in));
		
		$resultSet = new DB4DStatement($this->connection);
		$this->commandID=$this->commandID+2;
		$out = "";
		while(socket_last_error($this->connection)==0) {
			$char = socket_read($this->connection, 1);
			$out .=$char;
			if(strpos($out, "\r\n")===false) {continue;}
			if($resultSet->readStatementHeader($out)) {$out="";continue;}
			if($resultSet->readStatementResult()) {break;}
			else {throw new DB4DException("There was an error returning the result.",DB4DException::STMT_RETURN_ERROR);} 
		}
		return $resultSet;
	}

    /**
     * Rollback a transaction.
     * Revert all changes made during the ongoing transaction.
     * @throws DB4DException
     */
	public function rollback() {
		$this->query("ROLLBACK");
	}
}