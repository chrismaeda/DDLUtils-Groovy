package gddlutils.platform.oracle

import java.sql.Connection;
import java.sql.Types
import java.util.List;

import gddlutils.model.Column
import gddlutils.model.ColumnReference
import gddlutils.model.Database
import gddlutils.model.ForeignKey
import gddlutils.model.Index
import gddlutils.model.IndexColumn
import gddlutils.model.PrimaryKey
import gddlutils.model.Table
import gddlutils.model.TypeMap
import gddlutils.platform.Platform

class OraclePlatform extends Platform
{
	static Map StdTypeMap 
	
	static {
		StdTypeMap = [:]
		StdTypeMap[Types.CHAR] = "CHAR"
		StdTypeMap[Types.VARCHAR] = "VARCHAR2"
		StdTypeMap[Types.CLOB] = "CLOB"
		StdTypeMap[Types.DECIMAL] = "NUMBER"
		StdTypeMap[Types.NUMERIC] = "NUMBER"
	}
	
	public OraclePlatform()
	{
		// set name in superclass
		name = "ORACLE"
	}
	
	@Override
	public boolean hasStandardType(Column col)
	{
		String typeName = StdTypeMap[col.typeCode]
		if (typeName != null && typeName.equalsIgnoreCase(typeName)) {
			return true
		}
		return false
	}
	
	@Override
	public void gotTableModel(Database db, Table table) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void gotTableColumns(Database db, Table table)
	{
		// type-specific fixups for each column
		for (Column col in table.columns) {
			switch (col.typeCode) {
				case Types.DECIMAL:
					fixupDecimalColumn(db, table, col)
					break
				case Types.FLOAT:
					fixupFloatColumn(db, table, col)
					break
				case Types.DATE:
				case Types.TIMESTAMP:
					fixupDateTimeColumn(db, table, col)
					break
				case Types.ROWID:
					fixupRowidColumn(db, table, col)
					break
			}
	
			// handle column default		
			if (TypeMap.isTextType(col.typeCode)) {
				String colDefault = col.defaultVal				
				if (colDefault != null) {
					String modDefault = unescape(colDefault, "'", "''");
					if (! colDefault.equals(modDefault)) {
						col.defaultVal = modDefault
					}
				}
			}
				
			// If type is standard, then do not save
			// native type override info.  For example,
			// if JBDC type is VARCHAR and native type is VARCHAR2
			// then no need to save override info.  On the other
			// hand, if JDBC type is TIMESTAMP and native type
			// is DATE, we need to save this info since JDBC TIMESTAMP
			// can be represented by more than one Oracle type.  
			if (hasStandardType(col)) {
				col.typeName = null
			}
			// move native type code to platform map
			saveNativeColumnType(db, table, col)
		}
				
	}

	@Override
	public void gotTablePrimaryKeys(Database db, Table table) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void gotTablePrimaryKeys(Connection conn, Database db, Table table) {
		// TODO Auto-generated method stub

	}

	@Override
	public void gotTableForeignKeys(Database db, Table table) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void gotTableIndices(Database db, Table table) 
	{
		//
		// Oracle automatically creates a unique index for the primary key
		// so remove any indexes that have same name as primary key.  These
		// indices are not explicit parts of the schema so each platform should 
		// create them if necessary.
		//
		PrimaryKey pk = table.primaryKey
		String pkName = pk?.name
		
		if (pkName && table.indices != null && table.indices.containsKey(pkName)) {
			table.indices.remove(pkName)		
		}				
		
		// TODO Oracle does the same with UNIQUE constraints
		
		// TODO handle tablespaces
	}

	@Override
	public void gotTableIndices(Connection conn, Database db, Table table) {
		// TODO Auto-generated method stub

	}

	protected void fixupRowidColumn(Database db, Table table, Column col)
	{
		// rowid cols don't need size and scale
		col.size = null
		col.scale = null	
	}
	
	protected void fixupDecimalColumn(Database db, Table table, Column col)
	{
		if (col.size == null || col.size == 0) {
			// column type is "NUMBER" with no size and scale
			col.size = null
			col.scale = null
		}
		else {
			switch (col.size) {
				case 1:
					if (col.scale == null || col.scale == 0) {
						col.typeCode = Types.BIT
					}
					break
				case 3:
					if (col.scale == null || col.scale == 0) {
						col.typeCode = Types.TINYINT
					}
					break
				case 5:
					if (col.scale == null || col.scale == 0) {
						col.typeCode = Types.SMALLINT
					}
					break
					
				case 18:
					// IF NUMBER(18,0) THEN LEAVE TYPE AS DECIMAL
					if (col.scale > 0) {
						col.typeCode = Types.REAL
					}
					break;
				case 22:
					if (col.scale == 0) {
						col.typeCode = Types.INTEGER
					}
					break;
				case 38:
					if (col.scale == 0)
					{
						col.typeCode = Types.BIGINT
					}
					else
					{
						col.typeCode = Types.DOUBLE
					}
					break;
			}
		}
	}

	protected void fixupFloatColumn(Database db, Table table, Column col)
	{
		switch (col.size) {
			case 63:
				col.typeCode = Types.REAL
				break
			case 126:
				col.typeCode = Types.DOUBLE
				break
		}
	}
	
	protected void fixupDateTimeColumn(Database db, Table table, Column col)
	{
		// ddlutils code
		// 1. set DATE types to TIMESTAMP
		// 2. did weird manipulation with default values
		//
		// We don't do any of this right now.
		
		// size and scale are fixed
		col.size = null
		col.scale = null
	}

	@Override
	public List<String> generateTableDDL(Database db, Table table) 
	{
		List<String> stmts = []		
		StringBuffer buf = new StringBuffer()
		
		String tableType = table.type
		assert "TABLE".equalsIgnoreCase(tableType), "Cannot generate DDL for non-table " + table.name

		//
		// build create table
		//
		// add comment
		buf.append("\n\n/******\n  TABLE $table.name \n******/\n\n")
		buf.append("CREATE TABLE ")
		// TODO prepend schema to table name ???
		buf.append(table.name)
		buf.append(" ( \n")
		
		// generate column defs
		int colcnt = 0
		for (Column col in table.columns) {
			String colname = col.name
			String coltype = col.getTypeNameForDDL(this)
			String colsize = col.getTypeSizeForDDL(this)
			String coldefault = col.defaultVal
			boolean notnull = col.required
			
			if (colcnt > 0) {
				buf.append(",\n")
			}
			buf.append("\t$colname $coltype $colsize")
			if (coldefault) {
				buf.append(" DEFAULT $coldefault")
			}
			if (notnull) {
				buf.append(" NOT NULL")
			}
			colcnt++			
		}
		
		// primary key constraint
		if (colcnt > 0) {
			buf.append(",\n")
		}
		PrimaryKey pk = table.primaryKey
		if (pk != null) {
			String pkname = pk.name
			buf.append("\tCONSTRAINT $pkname\n\t  PRIMARY KEY (")
			int pkcolcnt = 0
			for (IndexColumn idxcol in pk.columns) {
				if (pkcolcnt > 0) {
					buf.append(", ")
				}
				buf.append(idxcol.columnName)
				pkcolcnt++
			}
			buf.append(")\n")
			// TODO ORACLE DDL PK INDEX TABLESPACE
		}
		
		buf.append(")")
		// TODO ORACLE DDL TABLESPACE
		stmts.add(buf.toString())
		
		//
		// create index statements
		//
		if (table.indices != null) {
			for (idxentry in table.indices.entrySet()) {
				String key = idxentry.key
				Index idx = idxentry.value
				
				buf.setLength(0)
				if (idx.unique) {
					buf.append("CREATE UNIQUE INDEX ")
				}
				else {
					buf.append("CREATE INDEX ")				
				}
				buf.append(idx.name)
				buf.append(" ON ")
				buf.append(table.name)
				buf.append("\n\t(")
				
				int idxcolcnt = 0
				for (IndexColumn idxcol in idx.columns) {
					if (idxcolcnt > 0) {
						buf.append(", ")
					}
					buf.append(idxcol.columnName)
					idxcolcnt++
				}
				buf.append(")")
				
				// TODO ORACLE DDL INDEX TABLESPACE
				
				stmts.add(buf.toString())
			}
		}
				
		return stmts
	}

	@Override
	public List<String> generateTableForeignKeyConstraints(Database db,	Table table) 
	{
		List<String> stmts = []
		StringBuffer buf = new StringBuffer()

		//
		// foreign key constraints
		//
		if (table.foreignKeys != null) { 
			int fkcnt = 0
			for (fkentry in table.foreignKeys.entrySet()) {
				ForeignKey fk = fkentry.value
				String fkname = fk.name
				String fktable = fk.foreignTableName
				
				// reset string buffer
				buf.setLength(0)
				
				if (fkcnt == 0) {
					buf.append("\n\n/******\n  Foreign Keys for TABLE $table.name \n******/\n\n")
					fkcnt++
				}
				buf.append("ALTER TABLE ")
				buf.append(table.name)
				buf.append("\n\tADD CONSTRAINT ")
				buf.append(fkname)
				buf.append("\n\tFOREIGN KEY (")
				
				// emit local column list
				int colcnt = 0
				for (ColumnReference colref in fk.references) {
					if (colcnt > 0) {
						buf.append(", ")
					}
					buf.append(colref.localColumnName)
					colcnt++
				}
				buf.append(")\n\tREFERENCES ")
				buf.append(fktable)
				buf.append(" (")
				// emit foreign column list
				colcnt = 0
				for (ColumnReference colref in fk.references) {
					if (colcnt > 0) {
						buf.append(", ")
					}
					buf.append(colref.foreignColumnName)
					colcnt++
				}
				buf.append(")")
	
				stmts.add(buf.toString())
			}
		}
				
		return stmts
	}

	@Override
	public String generateDDLTypeName(int typeCode) 
	{
		String typeName
		 
		if (StdTypeMap.containsKey(typeCode)) {
			typeName = StdTypeMap[typeCode]
		}
		else {
			// use default type map
			typeName = TypeMap.getTypeName(typeCode)	
		}
		
		return typeName
	}

	@Override
	public String generateDDLEndOfLine()
	{
		return ";"
	}
				
}
