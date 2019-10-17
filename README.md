4th Dimension Database Access
=============================
This is a pure in-place implementation of the native [PDO 4D](https://github.com/faldon/pdo_4d.git) driver 
for accessing a 4th Dimension database.

Since the native driver have some memory issues on certain operating systems (e.g. Debian 10 x64), where 
fetching rows results in a SEGFAULT while calling memcpy, this alternative implementation was written,
completely in PHP.

It has been tested against 4D Database v11-v13 in ASCII mode (fairly legacy systems).

# Usage
Connecting to your database system by creating a new DB4DDriver object:
```$db = new DB4DDriver($hostip, $dbport, $user, $password);```
Afterwards, the usage is similar to the usage of the PDO objects.

- ```$stmt = $db->prepare($sql);``` returns a DB4DStatement with a prepared query
- ```$stmt->execute($option_array);``` executes the statement, providing an optional array for binding values to the query
- ```$stmt->fetchRow($style);``` returns the next row as array in the given style
- ```$stmt->fetchAll($style);``` returns all rows as array of arrays in the given style

# Documentation
You can build the class documentation with phpdoc, using the provided phpdoc.xml.dist and according
the paths to your needs.
  