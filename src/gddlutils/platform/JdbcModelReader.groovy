package gddlutils.platform

import java.security.spec.PKCS8EncodedKeySpec;
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.ResultSet

import gddlutils.model.Column
import gddlutils.model.ColumnReference
import gddlutils.model.Database
import gddlutils.model.ForeignKey
import gddlutils.model.Index
import gddlutils.model.IndexColumn
import gddlutils.model.PrimaryKey
import gddlutils.model.Table
import gddlutils.platform.mysql.MySqlPlatform;

/**
 * Reverse Engineer Schema from JDBC Metadata
 * @author cmaeda
 *
 */
class JdbcModelReader 
{
	Connection conn
	DatabaseMetaData dbmeta
	Platform platform
	Database db
		
	/**
	 * Reads table definitions from metadata
	 * @param tabMeta
	 * @see java.sql.DatabaseMetaData#getTables(String, String, String, String[])
	 * @return
	 */
	def TreeMap<String,Table> getTables(String catalog, String schema, String tableNames)
	{
		TreeMap<String,Table> tables = new TreeMap<String,Table>()
		
		// fetch table metadata and read into table objects
		ResultSet tabMeta = null
		try {
			tabMeta = dbmeta.getTables(catalog, schema, tableNames, null)
			while (tabMeta.next()) 
			{
				Table newTab = getTable(tabMeta);
				tables[newTab.name] = newTab
			}
		}
		finally {
			if (tabMeta != null) {
				try { tabMeta.close() } catch (Exception x) {}
			}
		}
		
		// update db and do table callbacks if they have been set
		if (db != null) {
			db.tables = tables

			// invoke platform callback on each table
			if (platform != null) {
				for (tabentry in tables) {
					Table newTab = tabentry.value
					platform.gotTableModel(db, newTab)
				}
			}
		}

		// fetch table columns
		for (tabentry in tables) {
			Table newTab = tabentry.value
			getColumns(newTab)
			getPrimaryKeys(newTab)
			getIndices(newTab)
		}
		// fetch table foreign keys
		// all table columns need to be present before reading foreign keys
		for (tabentry in tables) {
			Table newTab = tabentry.value
			println "Get foreign keys for $newTab.name"
			getForeignKeys(newTab)
		}

		return tables		
	}
	
	/**
	 * Reads a single table definition from metadata
	 * @param tabMeta
	 * @see java.sql.DatabaseMetaData#getTables(String, String, String, String[])
	 * @return
	 */
	def Table getTable(ResultSet tabMeta)
	{
		// extract fields from metadata
		String tableCat = tabMeta.getString("TABLE_CAT")
		String tableSchema = tabMeta.getString("TABLE_SCHEM")
		String tableName = tabMeta.getString("TABLE_NAME")
		String tableType = tabMeta.getString("TABLE_TYPE")
		String tableDesc = tabMeta.getString("REMARKS")
		
		// create table object
		Table tab = new Table()
		tab.catalog = tableCat
		tab.schema = tableSchema
		tab.name = tableName
		tab.type = tableType
		tab.description = tableDesc
		
		return tab
	}

	/** 
	 * Fetches column metadata for table
	 * @see java.sql.DatabaseMetaData#getColumns(String, String, String, String)
	 * @param table
	 */
	def void getColumns(Table table)
	{
		println "getColumns( $table.name )"
		
		ResultSet colMeta = null
		List<Column> columns = []
		try {
			colMeta = dbmeta.getColumns(table.catalog, table.schema, table.name, null)
			while (colMeta.next()) 
			{
				Column col = getColumn(colMeta)
				columns.add(col)
			}
			// replace column list only if we read all columns successfully
			table.columns = columns
			
			// update db and do column callbacks if they have been set
			if (db != null && platform != null) {
				platform.gotTableColumns(db, table)
			}	
		}
		finally {
			if (colMeta != null) {
				try { colMeta.close() } catch (Exception x) {}
			}
		}		
	}
	
	def Column getColumn(ResultSet colMeta)
	{
		// read col fields
		//String tableCat = colMeta.getString("TABLE_CAT")
		//String tableSchema = colMeta.getString("TABLE_SCHEM")
		//String tableName = colMeta.getString("TABLE_NAME")

		// ddlutils source code says to read this column first in oracle
		String colDefault = colMeta.getString("COLUMN_DEF")
		String colName = colMeta.getString("COLUMN_NAME")
		String colComments = colMeta.getString("REMARKS")

		int colTypeCode = colMeta.getInt("DATA_TYPE")
		String colTypeName = colMeta.getString("TYPE_NAME")
		Integer colSize = colMeta.getInt("COLUMN_SIZE")
		if (colMeta.wasNull()) {
			colSize = null
		}
		Integer colScale = colMeta.getInt("DECIMAL_DIGITS")
		if (colMeta.wasNull()) {
			colScale = null
		}
		String colNullable = colMeta.getString("IS_NULLABLE")
		
		// documentation says we can do this, but it throws SQLException("Invalid column name")
		//String colAutoincrement = colMeta.getString("IS_AUTOINCREMENT")
		
		// build column object
		Column col = new Column()
		col.name = colName
		col.description = colComments
		
		col.typeCode = colTypeCode
		col.typeName = colTypeName
		col.size = colSize
		col.scale = colScale
		col.defaultVal = colDefault
		
		// nullable
		switch (colNullable) 
		{
			case "NO":
				col.required = true
				break
			case "YES":
			default:
				col.required = false
				break		
		}		
		
		// autoincrement
		/*
		switch (colAutoincrement) 
		{
			case "YES":
				col.autoIncrement = true
				break;
			default:
				col.autoIncrement = false
				break
		}
		*/
		
		return col
	}

	def void getPrimaryKeys(Table table)
	{
		println "getPrimaryKeys( $table.name )"
		
		ResultSet pkMeta = null
		PrimaryKey pk = null
		
		try {
			if (platform instanceof MySqlPlatform) {
				platform.gotTablePrimaryKeys(conn, db, table)

				return
			}

			pkMeta = dbmeta.getPrimaryKeys(table.catalog, table.schema, table.name)
			while (pkMeta.next())
			{
				String columnName = pkMeta.getString("COLUMN_NAME")
				short keySeq = pkMeta.getShort("KEY_SEQ")
				String pkName = pkMeta.getString("PK_NAME")
				
				Column column = table.getColumnByName(columnName)
				assert column != null, "Column $columnName does not exist"
				// column is part of primary key
				column.primaryKey = true
				
				// create pk object if necessary
				if (pk == null) {
					pk = new PrimaryKey()
					pk.name = pkName
				}
				IndexColumn idxCol = new IndexColumn()
				idxCol.ordinalPosition = keySeq
				idxCol.column = column
				idxCol.columnName = column.name
				pk.addColumn(idxCol)
			}
			// replace pk only if we read all columns successfully
			table.primaryKey = pk
			
			// update db and do column callbacks if they have been set
			if (db != null && platform != null) {
				platform.gotTablePrimaryKeys(db, table)
			}
		}
		finally {
			if (pkMeta != null) {
				try { pkMeta.close() } catch (Exception x) {}
			}
		}
	}

		
	/**
	 * Reads foreign key info from metadata
	 * @param table
	 * @see java.sql.DatabaseMetaData#getImportedKeys(String, String, String)
	 * @return
	 */
	def void getForeignKeys(Table table)
	{
		TreeMap<String,ForeignKey> fks = new TreeMap<String,ForeignKey>()
		ResultSet fkmeta = null
		
		try {
			fkmeta = dbmeta.getImportedKeys(table.catalog, table.schema, table.name)
			while (fkmeta.next()) {
				readForeignKey(fkmeta, table, fks)
			}
			table.foreignKeys = fks

			// update db and do column callbacks if they have been set
			if (db != null && platform != null) {
				platform.gotTableForeignKeys(db, table)
			}	
		}
		finally {
			if (fkmeta != null) {
				try { fkmeta.close() } catch (Exception x) {}
			}
		}
	}
	
	/**
	 * Reads a foreign key definition from result set.  Updates fk definitions in fks. 
	 * NOTE: each row is a single foreign key column.  An fk constraint can have multiple rows!
	 * 
	 * @param fkmeta
	 * @param table - the table that owns the foreign keys
	 * @param fks - list of known foreign keys for table
	 * @see java.sql.DatabaseMetaData#getImportedKeys(String, String, String)
	 * @return
	 */
	def void readForeignKey(ResultSet fkmeta, Table table, TreeMap<String,ForeignKey> fks)
	{
		// NOTE:
		// "fk" data refers to the local table
		// "pk" data refers to the foreign table		
		String pkTable = fkmeta.getString("PKTABLE_NAME")
		String pkColName = fkmeta.getString("PKCOLUMN_NAME")
		String fkTable = fkmeta.getString("FKTABLE_NAME")
		String fkColName = fkmeta.getString("FKCOLUMN_NAME")
		short keySeq = fkmeta.getShort("KEY_SEQ")
		short updateRule = fkmeta.getShort("UPDATE_RULE")
		short deleteRule = fkmeta.getShort("DELETE_RULE")
		String fkName = fkmeta.getString("FK_NAME")
		String pkName = fkmeta.getString("PK_NAME")

		//
		// validate table and column objects
		//
		// local table
		assert table.name.equalsIgnoreCase(fkTable), "Invalid table name $fkTable"
		Column localColumn = table.getColumnByName(fkColName)
		assert localColumn != null, "Column $fkTable.$fkColName does not exist"
		// foreign table
		Table foreignTable = db.tables[pkTable]
		assert foreignTable != null, "Foreign table $pkTable does not exist"
		Column foreignColumn = foreignTable.getColumnByName(pkColName)
		assert foreignColumn != null, "Foreign column $pkTable.$pkColName does not exist"
		
		// find or create fk object
		ForeignKey fk = fks[fkName]
		if (fk == null) {
			fk = new ForeignKey()
			fk.name = fkName
			fk.foreignTable = foreignTable
			fk.foreignTableName = foreignTable.name
			fk.localTable = table
			fk.localTableName = table.name
			fks[fkName] = fk
		}
		
		ColumnReference fkRef = new ColumnReference()
		fkRef.sequenceValue = keySeq
		fkRef.localColumn = localColumn
		fkRef.localColumnName = localColumn.name
		fkRef.foreignColumn = foreignColumn
		fkRef.foreignColumnName = foreignColumn.name
		fk.addRefence(fkRef)		
	}
	
	/**
	 * Fetches index data for table
	 * @see java.sql.DatabaseMetaData#getIndexInfo(String, String, String, boolean, boolean)
	 * @param table
	 */
	def void getIndices(Table table)
	{
		// do not get indices for view?		
		if ("VIEW".equalsIgnoreCase(table.type)
			&& platform != null 
			&& ! platform.fetchIndicesForView()) 
		{
			return
		}		
		println "getIndices( $table.name )"
		
		TreeMap<String,Index> indexes = new TreeMap<String,Index>()
		ResultSet idxmeta = null
		
		try {
			idxmeta = dbmeta.getIndexInfo(table.catalog, table.schema, table.name, false, false)
			while (idxmeta.next()) {
				readIndexColumn(idxmeta, table, indexes)
			}
			table.indices = indexes

			// update db and do column callbacks if they have been set
			if (db != null && platform != null) {
				if (platform instanceof MySqlPlatform) {
					platform.gotTableIndices(conn, db, table)
				} else {
					platform.gotTableIndices(db, table)
				}
			}
		}
		finally {
			if (idxmeta != null) {
				try { idxmeta.close() } catch (Exception x) {}
			}
		}
	}
	
	def void readIndexColumn(ResultSet idxmeta, Table table, TreeMap<String,Index> indexes)
	{
		boolean non_unique = idxmeta.getBoolean("NON_UNIQUE")
		String indexName = idxmeta.getString("INDEX_NAME")
		short indexType = idxmeta.getShort("TYPE")
		short ordinalPosition = idxmeta.getShort("ORDINAL_POSITION")
		String columnName = idxmeta.getString("COLUMN_NAME")
		
		if (indexName == null) {
			// no index name, nothing we can do
			return
		}
		
		Column column = table.getColumnByName(columnName)
		assert column != null, "Index Column $columnName does not exist"
		
		// find or create index
		Index idx = indexes[indexName]
		if (idx == null) {
			idx = new Index()
			idx.name = indexName
			idx.unique = (non_unique ? false : true)
			indexes[indexName] = idx
		}
		// add column to index
		IndexColumn idxCol = new IndexColumn()
		idxCol.ordinalPosition = ordinalPosition
		idxCol.column = column
		idxCol.columnName = column.name
		idx.addColumn(idxCol)
	}
}

 