<?php
error_reporting(E_ALL);

class MysqlDiff {
	/** @var object 连接对象 */
	protected $srcDb;
	/** @var string 数据库名称 */
	protected $srcDbName;
	/** @var object 连接对象 */
	protected $destDb;
	/** @var string 数据库名称 */
	protected $destDbName;
	/** $var mixed 输出文件 */
	protected $outputFile;

	public function __construct($conf) {
		$this->srcDbName = "src_schema_" . uniqid();
		$this->srcDb = $this->mysqlConnect($conf->db);
		$this->createDb($this->srcDb, $this->srcDbName);
		$this->srcDb->select_db($this->srcDbName);
		$this->loadSQL($conf->db, $this->srcDbName, $conf->srcSchemaFile);

		$this->destDbName = "dest_schema_" . uniqid();
		$this->destDb = $this->mysqlConnect($conf->db);
		$this->createDb($this->destDb, $this->destDbName);
		$this->destDb->select_db($this->destDbName);
		$this->loadSQL($conf->db, $this->destDbName, $conf->destSchemaFile);

		$this->outputFile = @fopen($conf->outputFile, 'w') or error("error creating output file {$conf->outputFile}");
	}

	public function __destruct() {
		if(isset($this->srcDb)){
			$this->dropSQL($this->srcDb, $this->srcDbName);
			$this->srcDb->close();
		}
		if(isset($this->destDb)){
			$this->dropSQL($this->destDb, $this->destDbName);
			$this->destDb->close();
		}
	}

	/**
	 * dropSQL 删除数据库
	 * @param mysql obj $conn
	 * @param string $dbName
	 */
	protected function dropSQL($conn, $dbName) {
		$sql = "drop database {$dbName}";
		$conn->query($sql);
	}
	
	/**
	 * loadSQL 加载数据库
	 * @param object $db
	 * @param string $dbName
	 * @param string $sqlFilePath
	 */
	protected function loadSQL($db, $dbName, $sqlFilePath) {
		$command = sprintf("mysql -h{$db->host} -u{$db->user} -p{$db->password} -D{$dbName} -Bse \"source $sqlFilePath\"");

		system($command, $retval);
		if($retval !== 0) {
			errorHandling(sprintf("{$command} error and error msg: %d", $retval));
		}
	}

	/**
	 * run 生成mysqldiff
	 * @return void
	 */
	public function run() {
		$sql = "";
		$srcTables = $this->listTables($this->srcDb, $this->srcDbName);
		$destTables = $this->listTables($this->destDb, $this->destDbName);

		// create tables
		$sql .= $this->cretaeTables($srcTables, $destTables);
		// drop tables
		$sql .= $this->dropTables($srcTables, $destTables);
		// alter tables
		$sql .= $this->alterTables($srcTables, $destTables);
		// alter columns
		$sql .= $this->alterColumns($srcTables, $destTables);
		// alter index
		$sql .= $this->alterIndex($srcTables, $destTables);
		// verify sql 
		$this->verifySql($sql);

		fputs($this->outputFile, $sql);
	}

	/**
	 * verifySql 验证sqlDiff
	 * @param string sql
	 * @return void
	 */
	protected function verifySql($sql) {
		$sqls = explode(";", $sql);
		foreach($sqls as $q){ 
			if(!trim($q)) {
				continue;
			}

			$rst = $this->destDb->query($q);
			if($rst === FALSE) {
				errorHandling(sprintf("{$q} error and error msg: %s", $this->destDb->error));
			}
		}
	}

	/**
	 * alterIndex 增量修改表
	 * @param array $srcTables
	 * @param array $destTables
	 * @return string
	 */	
	protected function alterIndex($srcTables, $destTables) {
		$sql = "";

		$intersectTables = array_intersect_key($srcTables, $destTables);
		foreach($intersectTables as $tableName => $tableInfo) {
			$srcIndexs = $this->listIndexs($tableName, $this->srcDb);
			$destIndexs = $this->listIndexs($tableName, $this->destDb);

			// drop index
			$sql .= $this->alterTableDropIndexs($srcIndexs, $destIndexs);
			// add index
			$sql .= $this->alterTableAddIndexs($srcIndexs, $destIndexs);
			// alter index
			$sql .= $this->alterTableAlterIndexs($srcIndexs, $destIndexs);
		}

		return $sql;
	}

	/**
	 * alterTableDropIndexs 删除键
	 * @param array $srcIndexs
	 * @param array $destIndexs
	 * @return string
	 */
	protected function alterTableDropIndexs($srcIndexs, $destIndexs) {
		$sql = "";

		$diffIndexes = array_diff_key($destIndexs, $srcIndexs);
		foreach ($diffIndexes as $indexName => $index) {
			$sql .= $this->buildDropIndexSql($index) . "\n\n";
		}
			
		return $sql;
	}

	/**
	 * buildDropIndexSql 生成删除键的sql
	 * @param object $index
	 * @return string
	 */
	protected function buildDropIndexSql($index) {
		return $index->key_name == 'PRIMARY' ?
			"ALTER TABLE {$index->table} DROP PRIMARY KEY;" :
			"ALTER TABLE {$index->table} DROP INDEX {$index->key_name};";
	}

	/**
	 * alterTableAddIndexs 增加键
	 * array $srcIndexs
	 * array $destIndexs
	 * @return string
	 */
	protected function alterTableAddIndexs($srcIndexs, $destIndexs) {
		$sql = "";

		$diffIndexes = array_diff_key($srcIndexs, $destIndexs);
		foreach($diffIndexes as $indexName => $index) {
			$sql .= $this->buildCreateIndexSql($index) . ";\n\n";
		}
		
		return $sql;
	}

	/**
	 * buildCreateIndexSql 创建键
	 * @param object $index
	 * @return string
	 */
	protected function buildCreateIndexSql($index) {
		$sql = "";
		$columns = array();

		foreach ($index->columns as $columnName => $columnInfos) {
			$columns[] = $columnName . ($columnInfos->sub_part ? "({$columnInfos->sub_part})" : "");
		}
		$columns = "(" . implode(",", $columns) . ")";

		if($index->key_name == 'PRIMARY') {
			$sql .= "ALTER TABLE {$index->table} ADD PRIMARY KEY {$columns}";
		} else {
			if($index->index_type == "FULLTEXT") {
				// 索引类型
				$indexType = "FULLTEXT";
			} elseif(!$index->non_unique) {
				$indexType = "UNIQUE";
			} else {
				$index_type = "";
			}

			$sql .= "CREATE {$indexType} INDEX {$index->key_name} ON {$index->table} {$columns}";
		}

		return $sql;
	}

	/**
	 * alterTableAlterIndexs 修改键
	 * array $srcIndexs
	 * array $destIndexs
	 * @return string
	 */
	protected function alterTableAlterIndexs($srcIndexs, $destIndexs) {
		$sql = "";

		$intersectIndexs = array_intersect_key($srcIndexs, $destIndexs);
		foreach ($intersectIndexs as $indexName => $index) {
			if(!$this->areIndexEq($index, $destIndexs[$indexName])) {
				// 把以前的index删了，增加新的
				$sql .= $this->buildDropIndexSql($destIndexs[$indexName]) . ";\n\n";
				$sql .= $this->buildCreateIndexSql($index) . ";\n\n";
			}
		}

		return $sql;		
	}

	/**
	 * areIndexEq 索引是否相等
	 * @param object $srcIndex
	 * @param object $destIndex
	 * @return boolean
	 */
	protected function areIndexEq($srcIndex, $destIndex) {
		if($srcIndex->index_type != $destIndex->index_type) {
			return FALSE;
		}

		if($srcIndex->non_unique != $destIndex->non_unique) {
			return FALSE;
		}

		if(count($srcIndex->columns) != count($destIndex->columns)) {
			return FALSE;
		}

		// 这种情况是不可能发生的，发生的话，直接报错出去
		if(empty($srcIndex->columns)) {
			errorHandling(sprintf("$srcIndex->columns is empty array and msg: %s", $srcIndex->table));
		}

		foreach ($srcIndex->columns as $name => $column) {
			if(!isset($destIndex->columns[$name])) {
				return FALSE;
			}

			if($column->seq_in_index != $destIndex->columns[$name]->seq_in_index) {
				return FALSE;
			}

			if($column->sub_part != $destIndex->columns[$name]->sub_part) {
				return FALSE;
			}

			if($column->index_type != $destIndex->columns[$name]->index_type) {
				return FALSE;
			}

			if($column->collation != $destIndex->columns[$name]->collation) {
				return FALSE;
			}

			if($column->comment != $destIndex->columns[$name]->comment) {
				return FALSE;
			}
		}

		return TRUE;
	}

	/**
	 * listIndexs 列出表的所有索引
	 * @param string $table
	 * @param obj $conn
	 * @return array
	 */
	protected function listIndexs($table, $conn) {
		$indexs = array();
		// 有可能是多个列对应一个索引
		$prevKeyName = NULL;

		$sql = "SHOW INDEXES FROM {$table}";
		$rst = $conn->query($sql);
		if($rst === FALSE) {
			errorHandling(sprintf("{$sql} error and error msg: %s", $conn->error));
		}

		while($row = $rst->fetch_object()) {
			$indexColumns = (object) array(
				/*
					索引的长度，如果是部分被编入索引，则该值表示索引的长度，
					如果是整列被编入索引则为NULL
				*/ 
				"sub_part" => $row->Sub_part,
				/*
					索引中序列的序列号，从1开始，如果是组合索引，那么按照字段在建立索引时的顺序排列，
					如('c1', 'c2', 'c3')那么分别为1，2，3
				*/
				"seq_in_index" => $row->Seq_in_index,
				/*
					所用索引方法（BTREE, FULLTEXT, HASH, RTREE）
				*/
				"index_type" => $row->Index_type,
				/*
					列以什么方式存储在索引中。在MySQL中，有值‘A’（升序）或NULL（无分序）。
				*/
				"collation" => $row->Collation,
				/*
					描述索引信息
				*/
				"comment" => $row->Comment
			);

			if($row->Key_name != $prevKeyName) {
				// 一个新的索引
				$indexs[$row->Key_name] = (object) array(
					"key_name" => $row->Key_name,
					"table" => $row->Table,
					/*
						如果该列索引中不包括重复的值则为0否则为1
					*/
					"non_unique" => $row->Non_unique,
					/*
						所用索引方法（BTREE, FULLTEXT, HASH, RTREE）
					*/
					"index_type" => $row->Index_type,
					"columns" => array(
						$row->Column_name => $indexColumns
					)
				);

				$prevKeyName = $row->Key_name;
			} else {
				// 多个列对应一个索引
				$indexs[$row->Key_name]->columns[$row->Column_name] = $indexColumns;
			}
		}

		return $indexs;
	}

	/**
	 * alterTables 增量修改表
	 * @param array $srcTables
	 * @param array $destTables
	 * @return string
	 */
	protected function alterTables($srcTables, $destTables) {
		$sql = "";

		$intersectTables = array_intersect(array_keys($srcTables), array_keys($destTables));
		foreach ($intersectTables as $table) {
			$st = $srcTables[$table];
			$dt = $destTables[$table];

			if($st->ENGINE != $dt->ENGINE) {
				$sql .= "ALTER TABLE {$table} ENGINE={$st->ENGINE};\n\n";
			}

			if($st->TABLE_COLLATION != $dt->TABLE_COLLATION) {
				$sql .= "ALTER TABLE {$table} COLLATE = {$st->TABLE_COLLATION}; \n\n";
			}

			if($st->TABLE_COMMENT != $dt->TABLE_COMMENT) {
				$sql .= "ALTER TABLE {$table} COMMENT = {$st->TABLE_COLLATION}; \n\n";
			}
		}

		return $sql;
	}

	/**
	 * alterColumns 增量修改列
	 * @param array $srcTables
	 * @param array $destTables
	 * @return string
	 */
	protected function alterColumns($srcTables, $destTables) {
		$sql = "";

		$intersectTables = array_intersect(array_keys($srcTables), array_keys($destTables));
		foreach ($intersectTables as $table) {
			$srcColumns = $this->listColumns($this->srcDb, $this->srcDbName, $table);
			$destColumns = $this->listColumns($this->destDb, $this->destDbName, $table);
			$columns_index = array_keys($srcColumns);

			foreach ($srcColumns as $column) {
				$afterColumn = $column->ORDINAL_POSITION == 1 ? NULL : $columns_index[$column->ORDINAL_POSITION - 2];
				if(!isset($destColumns[$column->COLUMN_NAME])) {
					$sql .= $this->alterTableAddColumn($column, $afterColumn, $column->TABLE_NAME);
				} else {
					$sql .= $this->alterTableModifyColumn($column, $destColumns[$column->COLUMN_NAME], $afterColumn, $column->TABLE_NAME);
				}
			}

			// alter drop columns
			$sql .= $this->alterDropColumns($srcColumns, $destColumns, $column->TABLE_NAME);
		}

		return $sql;
	}

	/**
	 * alterDropColumns 表的删除列
	 * @param array $srcColumns
	 * @param array $destColumns
	 * @param string $currentTable
	 * @return string
	 */
	protected function alterDropColumns($srcColumns, $destColumns, $currentTable) {
		$sql = "";

		$diffColumns = array_diff_key($destColumns, $srcColumns);
		foreach ($diffColumns as $column) {
			$sql .= "ALTER TABLE {$currentTable} DROP COLUMN {$column->COLUMN_NAME};\n\n";
		}

		return $sql;
	}

	/**
	 * alterTableAddColumn 表的新增列
	 * @param object $currentColumn
	 * @param string $afterColumn
	 * @param string $currentTable
	 * @return string 
	 */
	protected function alterTableAddColumn($currentColumn, $afterColumn, $currentTable) {
		$sql = "ALTER TABLE {$currentTable} ADD COLUMN {$currentColumn->COLUMN_NAME} " .
				$this->buildColumnDefinitionSql($currentColumn) . 
				($afterColumn ? " AFTER $afterColumn" : " FIRST") .
				";\n\n";

		return $sql;
	}

	/**
	 * alterTableChangeColumn 表的新增列
	 * @param object $currentColumn
	 * @param object $destColumn
	 * @param string $currentTable
	 * @param string $afterColumn
	 * @return string 
	 */
	protected function alterTableModifyColumn($currentColumn, $destColumn, $afterColumn, $currentTable) {
		$bModify = FALSE;
		$modify = array();
		$sql = "";

		if($currentColumn->COLUMN_TYPE != $destColumn->COLUMN_TYPE) {
			$bModify = TRUE;
			$modify["type"] = " {$currentColumn->COLUMN_TYPE}";
		}

		if($currentColumn->COLLATION_NAME != $destColumn->COLLATION_NAME) {
			$bModify = TRUE;
			$modify["collation"] = " COLLATE {$currentColumn->COLLATION_NAME}";
		}

		if($currentColumn->IS_NULLABLE != $destColumn->IS_NULLABLE) {
			$bModify = TRUE;
			$modify["nullable"] = strcasecmp($currentColumn->IS_NULLABLE, "NO") == 0 ? " NOT NULL" : " NULL";
		}

		if($currentColumn->COLUMN_DEFAULT != $destColumn->COLUMN_DEFAULT) {
			$bModify = TRUE;
			$modify["default"] = isset($currentColumn->COLUMN_DEFAULT) ? " DEFAULT " . $this->formatDefaultValue($currentColumn->COLUMN_DEFAULT) : FALSE;
		}		

		if($currentColumn->EXTRA != $destColumn->EXTRA) {
			$bModify = TRUE;
			$modify["extra"] = " {$currentColumn->EXTRA}";
		}

		if($currentColumn->COLUMN_COMMENT != $destColumn->COLUMN_COMMENT) {
			$bModify = TRUE;
			// 不用转义
			$modify["comment"] = " COMMENT {$currentColumn->COLUMN_COMMENT}";
		}

		if ($currentColumn->ORDINAL_POSITION != $destColumn->ORDINAL_POSITION && $bModify) {
			$modify["position"] = $afterColumn ? " AFTER {$afterColumn}" : " FIRST";
		}

		if($modify) {
			$sql .= "ALTER TABLE {$currentTable} MODIFY {$destColumn->COLUMN_NAME}";

			$sql .= isset($modify["type"]) ? $modify["type"] : " {$destColumn->COLUMN_TYPE}";

			if(isset($modify["collation"])) {
				$sql .= $modify["collation"];
			}
				
			if(isset($modify['nullable'])) {
				$sql .= $modify["nullable"];
			} else {
				$sql .= strcasecmp($destColumn->IS_NULLABLE, "NO") == 0 ? " NOT NULL" : " NULL";
			}

			if(isset($modify['default']) && $modify['default'] !== FALSE) {
				$sql .= $modify['default'];
			} elseif(isset($destColumn->COLUMN_DEFAULT)) {
				$sql .= ' DEFAULT ' . $this->formatDefaultValue($destColumn->COLUMN_DEFAULT);
			}

			if(isset($modify['extra'])) {
				$sql .= $modify['extra'];
			} elseif ($destColumn->EXTRA != '') {
				$sql .= " $destColumn->EXTRA";
			}

			if(isset($modify['comment'])) {
				$sql .= $modify['comment'];
			} elseif ($destColumn->COLUMN_COMMENT != '') {
				// 不用转义
				$sql .= " COMMENT '{$destColumn->COLUMN_COMMENT}'";
			}

			if(isset($modify['position'])) {
				$sql .= $modify['position'];
			}
			
			$sql .= ";\n\n";
		}

		return $sql;
	}

	/**
	 * buildColumnDefinitionSql 列的定义语句
	 * @param array $currentColumn
	 * @return string
	 */
	protected function buildColumnDefinitionSql($currentColumn) {
		$sql = $currentColumn->COLUMN_TYPE;

		if($currentColumn->COLLATION_NAME){
			$sql .= " COLLATE '$currentColumn->COLLATION_NAME'";
		}

		$sql .= strcasecmp($currentColumn->IS_NULLABLE, 'NO') == 0 ? ' NOT NULL' : ' NULL';

		if(isset($currentColumn->COLUMN_DEFAULT)){
			$sql .= ' DEFAULT ' . $this->formatDefaultValue($currentColumn->COLUMN_DEFAULT);
		}

		if($currentColumn->EXTRA) {
			$sql .= " {$$currentColumn->EXTRA}";
		}

		if($currentColumn->COLUMN_COMMENT) {
			$sql .= " COMMENT '" . $this->destDb->escape_string($currentColumn->COLUMN_COMMENT) . "'";
		}

		return $sql;
	}

	/**
	 * formatDefaultValue 
	 * @param string $columnDefaultValue
	 * @return string
	 */
	protected function formatDefaultValue($columnDefaultValue) {
		if(strcasecmp($columnDefaultValue, 'CURRENT_TIMESTAMP') == 0) {
			return $columnDefaultValue;
		} elseif(is_string($columnDefaultValue)) {
			return "'" . $this->destDb->escape_string($columnDefaultValue) . "'";
		} else {
			return $columnDefaultValue;
		}
	}

	/**
	 * listColumns 获取表的所有列
	 * @param object $conn
	 * @param string $dbName
	 * @param string $table
	 * @return array
	 */
	protected function listColumns($conn, $dbName, $table) {
		$columns = array();
		$sql = "SELECT * FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '{$dbName}' AND TABLE_NAME = '{$table}' ORDER BY ORDINAL_POSITION";

		$rst = $conn->query($sql);
		if($rst === FALSE) {
			errorHandling(sprintf("{$sql} error and error msg: %s", $conn->error));
		}

		while($row = $rst->fetch_object()) {
			$columns[$row->COLUMN_NAME] = $row;
		}

		return $columns;
	}

	/**
	 * cretaeTables 应该增加的表
	 * @param array $srcTables
	 * @param array $destTables
	 * @return string
	 */
	protected function cretaeTables($srcTables, $destTables) {
		$sql = "";

		$diffTables = array_diff(array_keys($srcTables), array_keys($destTables));
		foreach ($diffTables as $table) {
			$sql .= $this->getCreateTableSql($table) . ";\n\n"; 
		}

		return $sql;
	}

	/**
	 * dropTables 应该删除的表
	 * @param array $srcTables
	 * @param array $destTables
	 * @return string
	 */
	protected function dropTables($srcTables, $destTables) {
		$sql = "";

		$diffTables = array_diff(array_keys($destTables), array_keys($srcTables));
		foreach ($diffTables as $table) {
			$sql .= "DROP TABLE {$table}; \n\n";
		}

		return $sql;
	}
	/**
	 * getCreateTableSql 创建表
	 * @param string $table
	 * @return string 
	 */
	protected function getCreateTableSql($table) {
		$sql = "SHOW CREATE TABLE {$table}";
		$rst = $this->srcDb->query($sql);
		if($rst === FALSE) {
			errorHandling(sprintf("{$sql} error and error msg: %s", $this->srcDb->error));
		}

		$data = $rst->fetch_row();
		return $data[1];
	}

	/**
	 * listTables 返回库中的所有表
	 * @param object $conn
	 * @param string $dbName
	 * @return array
	 */
	protected function listTables($conn, $dbName) {
		$tables = array();
		// 表的名称，表的引擎，	表的字符校验编码集，表的注释
		$sql = "SELECT TABLE_NAME, ENGINE, TABLE_COLLATION, TABLE_COMMENT FROM information_schema.TABLES WHERE table_schema = '{$dbName}'";
		$rst = $conn->query($sql);
		if($rst === FALSE) {
			errorHandling(sprintf("{$sql} error and error msg: %s", $conn->error));
		}

		while($row = $rst->fetch_object()) {
			$tables[$row->TABLE_NAME] = $row;
		}
		$rst->close();

		return $tables;
	}

	/**
	 * mysqlConnect 连接mysql
	 * @param object $db
	 * @return object
	 */
	protected function mysqlConnect($db) {
		// 创建连接
		$mysqli = new mysqli($db->host, $db->user, $db->password);
		// 是否连接成功
		if(mysqli_connect_error()) {
			errorHandling(sprintf("connect mysql error by host:%s, user:%s, password:%s", $db->host, $db->user, $db->password));
		}

		return $mysqli;
	}

	/**
	 * createDb 创建数据库
	 * @param mysql obj $conn
	 * @param string $dbname
	 */
	protected function createDb($conn, $dbname) {
		$sql = "CREATE DATABASE {$dbname} DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci";
		if($conn->query($sql) === FALSE) {
			errorHandling(sprintf("{$sql} error and error msg: %s", $conn->error));
		}
	}
}

function errorHandling($msg) {
	print_r($msg);
	print_r("\n");
	exit(1);
}

function usage() {
echo <<<MSG
Usage:
	php mysqldiff.php <options>

Options:
	-c <config-file> config file path
	
	-h <hostname> Server hosting source db.
	   Default hostname is 'localhost'
	-u <username> Username for connectiong to source db.
	-p <password> Password for connectiong to source db.

	-s <src-schema-file> Filename of the file which contain the db schema in sql
	-d <dest-schema-file2> Filename of the file which contain the db schema in sql	

	-o <out-filename> Filename to save the generated MySQL script.
	   Default is to write to STDOUT.

Example:
	php mysqldiff.php -c config.php [-o out-filename]
	php mysqldiff.php [-h hostname] -u user -p password -s schema-file1.sql -d schema-file2.sql [-o out-filename]\n
MSG;
	exit(1);
}

function main() {
	global $argc, $argv, $conf;

	if($argc == 1) {
		usage();
	}

	for($i=1; $i<$argc; ++$i) {
		switch($argv[$i]) {
			case '--help':
				usage();
				break;
			case '-c':
				$conf = require $argv[++$i];
				break;
				break;
			case '-h':
				$conf->db->host = $argv[++$i];
				break;
			case '-u':
				$conf->db->user = $argv[++$i];
				break;
			case '-p':
				$conf->db->password = $argv[++$i];
				break;
			case '-s':
				$conf->srcSchemaFile = $argv[++$i];
				break;
			case '-d':
				$conf->destSchemaFile = $argv[++$i];
				break;
			case '-o':
				$conf->outputFile = $argv[++$i];
				break;
			default:
				usage();
		}
	}

	$md = new MysqlDiff($conf);
	$md->run();
	
	exit(0);
}

$conf = (object)array(
	'db' => (object)array(
		'host' => 'localhost',
		'user' => NULL,
		'password' => NULL,
	),
	'schemaFile1' => NULL,
	'schemaFile2' => NULL,
	'outputFile' => 'php://stdout'
);

main();