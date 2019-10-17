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
use InvalidArgumentException;

/**
 * Handle query results of a 4th Dimension database.
 * This class reads the response of a previously executet sql query.
 * It provides methods to access the data of a result as well as access metadata of the result.
 * @package DB4D
 */
class DB4DStatement {
	/**
	 * The result type of the statement.
	 * Stores the type of the expected statement result. Could be either Update-Count or Fetch-Result.
	 * @access public
	 * @var String
	 */
	public $resultType;
	/**
	 * Total count of affected rows.
	 * @access public
	 * @var integer
	 */
	public $rowCount;
	/**
	 * Total count of columns.
	 * @access private
	 * @var integer
	 */	
	private $columnCount;
	/**
	 * The result set's column aliases.
	 * @access private
	 * @var array
	 */
	private $columnNames;
	/**
	 * The datatypes of the returned columns.
	 * @access private
	 * @var array
	 */
	private $columnTypes;
	/**
	 * The updateability of the returned columns.
	 * @access private
	 * @var array
	 */	
	private $columnUpdateability;
	/**
	 * The count of the executed command.
	 * @access private
	 * @var integer
	 */
	private $commandCount;
	/**
	 * The connection to a 4th Dimension database.
	 * @access private
	 * @var resource
	 */
	private $connectionResource;
	/**
	 * A compiled query to be executed with optional arguments.
	 * @access private
	 * @var String
	 */
	private $preparedQuery;
	/**
	 * The amount of rows sent as initial response
	 * @access private
	 * @var integer
	 */
	private $rowCountSent;
	/**
	 * The result returned by the server.
	 * @access private
	 * @var array
	 */
	private $row;
	/**
	 * The id of the statement.
	 * @access private
	 * @var integer
	 */
	private $statementID;
	/**
	 * The id of the command.
	 * @access private
	 * @var integer
	 */
	private $commandID;
	/**
	 * Is an error occurred?
	 * @access private
	 * @var bool
	 */
	private $error;
	/**
	 * The error code of the error.
	 * @access private
	 * @var string
	 */
	private $errorCode;
	/**
	 * The error component code of the error.
	 * @access private
	 * @var string
	 */
	private $errorComponentCode;
	/**
	 * The error description of the error.
	 * @access private
	 * @var string
	 */
	private $errorDescription;
	
	/**
	 * Preparing the arguments of a query.
	 * @param mixed $value The value to be prepared.
	 */
	public static function parseArguments(&$value) {
		if(is_null($value)) {
			$value = "NULL";
			return;
		}
		if(is_string($value)) {
			$value = str_replace(chr(10), '', str_replace(chr(13), '', $value));
			$value = str_replace("'", "''", $value);
			$value = str_replace("?", ":QUOT:", $value);
			$value = "'{$value}'";
			return;
		}
		if(is_bool($value)) {
			$value = "CAST(" . intval($value) . " as BOOLEAN)";
			return;
		}
	}

	/**
	 * Create a DB4DStatement object.
	 * Ceates a new DB4DStatement object and set it's prepared query.
	 * @param resource $dbConnection An established connection to a 4D server.
	 * @param String $preparedQuery A complete message ready to send out for executing.
	 * @param int $commandID The id for the initial command.
	 */
	public function __construct(&$dbConnection, $preparedQuery=NULL, $commandID=3) {
		$this->connectionResource = $dbConnection;
		$this->preparedQuery = $preparedQuery;
		$this->commandID = $commandID;
	}
	
	/**
	 * The magic setter method
	 * Sets the value of a property or throws an exception if the
	 * property does not exist.
	 * @param string $property The name of the property to set
	 * @param string $value The value of the property to set
	 * @throws InvalidArgumentException if the property does not exist
	 */
	public function __set($property, $value) {
		if(property_exists($this, $property)) {$this->$property=$value;}
		else {throw new InvalidArgumentException('This property does not exists.');}
	}
	
	/**
	 * The magic getter method
	 * Gets the value of a property or throws an exception if the
	 * property does not exist.
	 * @param string $property The name of the property to get
	 * @return string The value of the property
	 * @throws InvalidArgumentException if the property does not exist
	 */
	public function __get($property) {
		if(property_exists($this, $property)) {return $this->$property;}
		else {throw new InvalidArgumentException('This property does not exists.');}
	}
	
	/**
	 * Close the database cursor of the statement.
	 */
	public function closeCursor() {
		$in = "009 CLOSE-STATEMENT\r\nSTATEMENT-ID : {$this->statementID}\r\n\r\n";
		socket_write($this->connectionResource, $in, strlen($in));
		return socket_read($this->connectionResource, 9);
	}
	
	/**
	 * Execute a query.
	 * Parses the arguments and injects them into the prepared query before executing it.
	 * @param array $args The array with the optional arguments of the query.
	 * @return mixed Boolean success of a SELECT statement. On INSERT, UPDATE and DELETE statements, the number of rows affected.
	 * @throws DB4DException 
	 */
	public function execute($args=NULL) {
		if($args!==NULL && (!is_array($args) || substr_count($this->preparedQuery, "?")!=count($args))) {
			throw new DB4DException("Count of arguments does not match.", DB4DException::STMT_UNMATCHING_ARGS);
		}
		else {
			$in = $this->preparedQuery;
			if($args!==NULL) {
				array_walk($args, 'UhuniBundle\External\DB4D\DB4DStatement::parseArguments');
				$in = preg_replace(array_fill(0, count($args), "/\?/"), $args, $this->preparedQuery, 1);
				$in = str_replace(":QUOT:", "?", $in);
			}
			socket_write($this->connectionResource, $in, strlen($in));

			$out = "";
			while(socket_last_error($this->connectionResource)==0) {
				$char = socket_read($this->connectionResource, 1);
				$out .=$char;
				if(strpos($out, "\r\n")===false) {continue;}
				if($this->readStatementHeader($out)) {$out="";continue;}
				if($this->errorOccured()) {
					throw new DB4DException("There was an error with the 4D statement: {$this->getErrorCode()}\n{$this->getErrorDescription()}", DB4DException::STMT_RETURN_ERROR);
				}
				if($this->resultType=="Update-Count \r\n") {
					return $this->rowCount;
				}
				if($this->rowCount==0) {return false;}
				if($this->readStatementResult()) {break;}
				else {break;}
			}
			unset($out);
			
			$this->row = array();
			$in = str_replace(str_pad($this->commandID, 3, "0", STR_PAD_LEFT)." EXECUTE-STATEMENT", str_pad($this->commandID+2, 3, "0", STR_PAD_LEFT)." EXECUTE-STATEMENT", $in);
			$in = str_replace("FIRST-PAGE-SIZE : 1", "FIRST-PAGE-SIZE : {$this->rowCount}", $in);
			
			socket_write($this->connectionResource, $in, strlen($in));

			$out = "";
			while(socket_last_error($this->connectionResource)==0) {
				$char = socket_read($this->connectionResource, 1);
				$out .=$char;
				if(strpos($out, "\r\n")===false) {continue;}
				if($this->readStatementHeader($out)) {$out="";continue;}
				if($this->readStatementResult()) {break;}
				else {throw new DB4DException("There was an error returning the result.",DB4DException::STMT_RETURN_ERROR);} 
			}
			return true;
		}
	}
	
	/**
	 * Fetch all records of the result set.
	 * @param int $style Defines the style of the returning array.
	 * @return array A numeric array of numeric, associative or (default) numeric-associative arrays of the result set.
	 */
	public function fetchAll($style=DB4DDriver::FETCH_BOTH) {
		if($this->row===NULL) {return $this->row;}
		switch($style) {
			case DB4DDriver::FETCH_NUM:
				foreach($this->row as &$row) {
					for($i=0; $i<count($this->columnNames); $i++) {
						$row[$i] = $row[$this->columnNames[$i]];
						unset($row[$this->columnNames[$i]]);
					}
				}
				$result = $this->row;
				break;
			case DB4DDriver::FETCH_ASSOC:
				$result = $this->row;
				break;
			default:
				foreach($this->row as &$row) {
					for($i=0; $i<count($this->columnNames); $i++) {
						$row[$i] = $row[$this->columnNames[$i]];
						unset($row[$this->columnNames[$i]]);
						$row[$this->columnNames[$i]] = $row[$i];
					}
				}
				$result = $this->row;
				break;
		}
		return $result;
	}
	
	/**
	 * Fetch a column of the result set.
	 * Fetch a column of the result set and advance the cursor by one row.
	 * @param int $index The index of the column to fetch.
	 * @return mixed | boolean The data of the column in the result set or FALSE, if there is no result.
	 */
	public function fetchColumn($index=0) {
		if($this->row===NULL) {return false;}
		$row = array_shift($this->row);
		return $row[$this->columnNames[$index]];
	}
	
	/**
	 * Fetch a row of the result set.
	 * Fetch a row of the result set and advance the cursor by one row.
	 * @param int $style Defines the style of the returning array.
	 * @return array | null The row of the result set as array, depending on the constant array style or NULL if the result set is empty.
	 */
	public function fetchRow($style=DB4DDriver::FETCH_BOTH) {
		if($this->row===NULL) {return $this->row;}
		switch($style) {
			case DB4DDriver::FETCH_NUM:
				$tmp = array_shift($this->row);
				$row = array();
				if($tmp!==NULL) {
					foreach ($tmp as $column => $value) {
                        array_push($row, array_shift($tmp));
                        if($column=="_ID") {
                            array_pop($row);
                        }
					}
				}
				else { $row=NULL; }
				break;
			case DB4DDriver::FETCH_ASSOC:
				$row = array_shift($this->row);
                unset($row["_ID"]);
				break;
			default:
				$tmp = array_shift($this->row);
				$row = array();
				if($tmp!==NULL) {
					for($i=0; $i<count($this->columnNames); $i++) {
						$row[$i] = $tmp[$this->columnNames[$i]];
						$row[$this->columnNames[$i]] = $tmp[$this->columnNames[$i]];
					}
				}
				else { $row=NULL; }
				break;
		}
		return $row;
	}
	
	/**
	 * Read the names of the affected colums.
	 * @return array a numerical array containing the column names.
	 */
	public function readMetadata() {
		return $this->columnNames;
	}

	public function errorOccured() {
		return $this->error;
	}

	public function getErrorCode() {
		return str_replace(array("\r", "\n"), '', $this->errorCode);
	}

	public function getErrorComponentCode() {
		return str_replace(array("\r", "\n"), '', $this->errorComponentCode);
	}

	public function getErrorDescription() {
		return str_replace(array("\r", "\n"), '', $this->errorDescription);
	}

    /**
     * Get sql code of a query.
     * Parses the arguments and injects them into the prepared query before returning it.
     * @param array $parameters The array with the optional arguments of the query.
     * @return string The runnable sql command
     * @throws DB4DException
     */
    public function getRawSql($parameters=NULL) {
        if($parameters!==NULL && (!is_array($parameters) || substr_count($this->preparedQuery, "?")!=count($parameters))) {
            throw new DB4DException("Count of arguments does not match.", DB4DException::STMT_UNMATCHING_ARGS);
        }
        else {
            $in = $this->preparedQuery;
            if ($parameters !== null) {
                array_walk($parameters, 'UhuniBundle\External\DB4D\DB4DStatement::parseArguments');
                $in = preg_replace(array_fill(0, count($parameters), "/\?/"), $parameters, $this->preparedQuery, 1);
                $in = str_replace(":QUOT:", "?", $in);
            }
            $sql = substr($in, 35, strlen($in)-85).";";
            return $sql;
        }
    }

	public function getStatementID() {
		return $this->statementID;
	}
	
	/**
	 * Read the header of the result set.
	 * @param String $line A CRLF terminated input string.
	 * @return boolean TRUE if the input string was succesfully parsed, else FALSE.
	 */
	public function readStatementHeader($line) {
		if(strcmp($line, "\r\n")===0) { return false; }
		if(strpos($line, " OK")!==false) { $this->error = false; return true; }
		else if(strpos($line, " ERROR")!==false) { $this->error = true; return true; }
			$aTmp = explode(":", $line);
			switch($aTmp[0]) {
				case("Statement-ID"):
					$this->statementID=intval($aTmp[1]);
					return true;
					break;
				case("Command-Count"):
					$this->commandCount=intval($aTmp[1]);
					return true;
					break;
				case("Result-Type"):
					$this->resultType=$aTmp[1];
					return true;
					break;
				case("Column-Count"):
					$this->columnCount=intval($aTmp[1]);
					return true;
					break;
				case("Row-Count"):
					$this->rowCount=intval($aTmp[1]);
					return true;
					break;
				case("Column-Types"):
					$this->columnTypes=explode(" ",$aTmp[1],-1);
					return true;
					break;
				case("Column-Aliases"):
					$this->columnNames=str_replace(array(" [", "[", "]"), "", explode("]", $aTmp[1], -1));
					return true;
					break;
				case("Column-Updateability"):
					$this->columnUpdateability=explode(" ",$aTmp[1]);
					array_shift($this->columnUpdateability);
					return true;
					break;
				case("Row-Count-Sent"):
					$this->rowCountSent=intval($aTmp[1]);
					return true;
					break;
				case("Error-Code"):
					$this->errorCode=$aTmp[1];
					return true;
					break;
				case("Error-Component-Code"):
					$this->errorComponentCode=$aTmp[1];
					return true;
					break;
				case("Error-Description"):
					$this->errorDescription=$aTmp[1];
					return true;
					break;
			}
			return false;
	}

    /**
     * Read the data of the result set.
     * @return boolean TRUE, if rowCount equals rows sent, else FALSE
     */
	public function readStatementResult() {
		if($this->row===null) {
			$rowCount = 0;
		} else {
			$rowCount=count($this->row);
		}
		while(socket_last_error($this->connectionResource)==0) {
			if(in_array("Y\r\n", $this->columnUpdateability)) {
				socket_read($this->connectionResource, 1);
				$this->row[$rowCount]["_ID"] = ord(socket_read($this->connectionResource, 1))*pow(256,0)+ord(socket_read($this->connectionResource, 1))*pow(256,1)+ord(socket_read($this->connectionResource, 1))*pow(256,2)+ord(socket_read($this->connectionResource, 1))*pow(256,3);
			};

			for($i=0;$i<$this->columnCount;$i++) {
				$char = socket_read($this->connectionResource, 1);
				// 1=Non-Null Value, 0=Null Value, 2=Error
				if($char==0) { continue; }
				elseif($char==1) {
					switch($this->columnTypes[$i]) {
						case "VK_BOOLEAN":
							$this->row[$rowCount][$this->columnNames[$i]] = $this->readinBool();
							break;
						case "VK_BLOB":
							$this->row[$rowCount][$this->columnNames[$i]] = $this->readinBlob();
							break;
						case "VK_BYTE":
							$this->row[$rowCount][$this->columnNames[$i]] = $this->readinByte();
							break;
						case "VK_TIMESTAMP":
							$this->row[$rowCount][$this->columnNames[$i]] = $this->readinTime();
							break;
						case "VK_DURATION":
							$this->row[$rowCount][$this->columnNames[$i]] = $this->readinDuration();
							break;
						case "VK_FLOAT":
							$this->row[$rowCount][$this->columnNames[$i]] = $this->readinFloat();
							break;
						case "VK_IMAGE":
							$this->row[$rowCount][$this->columnNames[$i]] = $this->readinImage();
							break;
						case "VK_LONG":
							$this->row[$rowCount][$this->columnNames[$i]] = $this->readinLong();
							break;
						case "VK_LONG8":
							$this->row[$rowCount][$this->columnNames[$i]] = $this->readinLong8();
							break;
						case "VK_REAL":
							$this->row[$rowCount][$this->columnNames[$i]] = $this->readinReal();
							break;
						case "VK_TIME":
							$this->row[$rowCount][$this->columnNames[$i]] = $this->readinTime();							
							break;
						case "VK_STRING":
							$this->row[$rowCount][$this->columnNames[$i]] = $this->readinString();
							break;
						case "VK_WORD":
							$this->row[$rowCount][$this->columnNames[$i]] = $this->readinByte();
							break;
					}
				}
				elseif($char==2) {
					$errorCode = socket_read($this->connectionResource, 8);
					echo $errorCode;
					exit();
				}
			}
			$rowCount++;
			if(($rowCount)==$this->rowCountSent) {
				if(count($this->row)<$this->rowCount) {return false;}
				elseif(count($this->row)==$this->rowCount) {return true;}
				else {return false;}
			};
		}
        return false;
	}
	
	/**
	 * Decode Blob.
	 * @return string
	 */
	private function readinBlob() {
		$length = $this->readinLong();
        $data = socket_read($this->connectionResource, $length);
        return $data;
	}
	
	/**
	 * Decode Boolean.
	 * @return boolean
	 */
	private function readinBool() {
		return (boolean)ord(socket_read($this->connectionResource, 1))*pow(256,0)+ord(socket_read($this->connectionResource, 1))*pow(256,1);
	}
	
	/**
	 * Decode Byte.
	 * @return integer
	 */
	private function readinByte() {
		return ord(socket_read($this->connectionResource, 1))*pow(256,0)+ord(socket_read($this->connectionResource, 1))*pow(256,1);
	}

	/**
	 * Decode Time
	 * @return string
	 */
	private function readinDuration() {
		return $this->readinLong8();
	}
	
	/**
	 * Decode Float.
	 * @return float
	 */
	private function readinFloat() {
		$exp = $this->readinLong();
		$sign = (int)socket_read($this->connectionResource, 1);
        $dataLength = $this->readinLong();
		$data = (int)socket_read($this->connectionResource, $dataLength);
		return pow(-1, $sign)*(1+$data*pow(2, -23))*pow(2, $exp-127);
	}
	
	/**
	 * Decode Image.
     * @return string
	 */
	private function readinImage() {
        return $this->readinBlob();
	}
	
	/**
	 * Decode Long.
	 * @return integer
	 */
	private function readinLong() {
		return ord(socket_read($this->connectionResource, 1))*pow(256,0)+ord(socket_read($this->connectionResource, 1))*pow(256,1)+ord(socket_read($this->connectionResource, 1))*pow(256,2)+ord(socket_read($this->connectionResource, 1))*pow(256,3);
	}
	
	/**
	 * Decode Long8.
	 * @return integer
	 */
	private function readinLong8() {
		return ord(socket_read($this->connectionResource, 1))*pow(256,0)+ord(socket_read($this->connectionResource, 1))*pow(256,1)+ord(socket_read($this->connectionResource, 1))*pow(256,2)+ord(socket_read($this->connectionResource, 1))*pow(256,3)+ord(socket_read($this->connectionResource, 1))*pow(256,4)+ord(socket_read($this->connectionResource, 1))*pow(256,5)+ord(socket_read($this->connectionResource, 1))*pow(256,6)+ord(socket_read($this->connectionResource, 1))*pow(256,7);
	}
	
	/**
	 * Decode Real.
	 * @return float
	 */
	private function readinReal() {
		return unpack("d", socket_read($this->connectionResource, 8))[1];
	}
	
	/**
	 * Decode String.
	 * @return string UTF-16 converted string
	 */
	private function readinString() {
		$length=pow(256, 4)-$this->readinLong();
		if($length==pow(256, 4)) {
			return "";
		}
		$s = mb_convert_encoding(socket_read($this->connectionResource, 2*$length), 'UTF-8', 'UTF-16LE');
		return $s;
	}
	
	/**
	 * Decode Date.
	 * @return string
	 */
	private function readinTime() {
		$year = ord(socket_read($this->connectionResource, 1))*pow(256, 0)+ord(socket_read($this->connectionResource, 1))*pow(256, 1);
		$month = ord(socket_read($this->connectionResource, 1));
		$day = ord(socket_read($this->connectionResource, 1));
		$milis = $this->readinLong();
		if ($milis==0) {
			return str_pad($day, 2, "0", STR_PAD_LEFT).".".str_pad($month, 2, "0", STR_PAD_LEFT).".".str_pad($year, 4, "0", STR_PAD_LEFT);
		}
		$hours = $milis/(3600*1000);
		$minutes = ($milis-($hours*3600*1000))/(60*1000);
		$seconds = ($milis-($hours*3600*1000)-($minutes*60*1000))/1000;
		return str_pad($day, 2, "0", STR_PAD_LEFT).
				".".
				str_pad($month, 2, "0", STR_PAD_LEFT).
				".".
				str_pad($year, 4, "0", STR_PAD_LEFT).
				" ".
				str_pad($hours, 2, "0", STR_PAD_LEFT).
				":".
				str_pad($minutes, 2, "0", STR_PAD_LEFT).
				":".
				str_pad($seconds, 2, "0", STR_PAD_LEFT);
	}
}