package gddlutils.platform.mssql

import java.awt.event.ItemEvent;
import java.sql.Connection;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import gddlutils.model.Column;
import gddlutils.model.ColumnReference;
import gddlutils.model.Database;
import gddlutils.model.ForeignKey;
import gddlutils.model.Index;
import gddlutils.model.IndexColumn;
import gddlutils.model.PrimaryKey;
import gddlutils.model.Table;
import gddlutils.model.TypeMap;
import gddlutils.platform.Platform;

class MSSqlPlatform extends Platform {

	static Map stdTypesMap

	static {
		stdTypesMap = [:]
	}

	public MSSqlPlatform() {
		name = "SQL Server"
	}

	@Override
	public void gotTableModel(Database db, Table table) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void gotTableColumns(Database db, Table table) {

		for (Column col in table.columns) {

			switch (col.typeCode) {
				case Types.TINYINT:
				case Types.SMALLINT:
				case Types.INTEGER:
				case Types.REAL:
					col.size = null
					break
				case Types.DECIMAL:
					break
				case Types.NUMERIC:
					break
				case Types.FLOAT:
				case Types.DOUBLE:
					break
				case Types.DATE:
				case Types.TIMESTAMP:
					fixupDateTimeColumn(db, table, col)
					break
			}
			
			/*
			 * If a data type found in MySql exists in JDBC with same name then
			 * there is not issue. Don't write type name in native type names.
			 *
			 * But if the type does not found with the same name with JDBC then
			 * make it save a native column type.
			 */
			if (hasStandardType(col)) {
				col.typeName = null
			}
			
			// move native type code to platform map
			saveNativeColumnType(db, table, col)
		}
	}

	protected void fixupDateTimeColumn(Database db, Table table, Column col) {
		col.size = null
		col.scale = null
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
	public void gotTableIndices(Database db, Table table) {

		/* 
		 * Removes indexes for the table if index name is of the same name as Primary Key name.
		 * Compares name of primary key with key names of indexes.
		 */

		PrimaryKey primaryKey = table.primaryKey
		
		if (primaryKey != null) {
			Index indx = table.indices?.remove(primaryKey.name)

			println "$indx.name Removed"
		}
	}

	@Override
	public void gotTableIndices(Connection conn, Database db, Table table) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean hasStandardType(Column col) {
		String typeName = stdTypesMap[col.typeCode]

		if (typeName != null && typeName.equalsIgnoreCase(col.typeName)) {
			return true
		}

		return false
	}

	@Override
	public List<String> generateTableDDL(Database db, Table table) {
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

			// TODO: Check if we can remove this check.
			if (coltype.equalsIgnoreCase("ntext")) {
				colsize = ""
			}

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
	public List<String> generateTableForeignKeyConstraints(Database db, Table table) {

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
	public String generateDDLTypeName(int typeCode) {
		String typeName

		if (stdTypesMap.containsKey(typeCode)) {
			typeName = stdTypesMap[typeCode]
		} else {
			typeName = TypeMap.getTypeName(typeCode)	
		}

		if (typeCode == 4) {
			println "Type Name : " + typeName
		}
		return typeName
	}

	@Override
	public String generateDDLEndOfLine() {
		return ";"
	}

}
