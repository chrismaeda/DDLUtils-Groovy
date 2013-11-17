package gddlutils.platform

import gddlutils.model.Column
import gddlutils.model.Database
import gddlutils.model.Table

/**
 * Platforms are intended to be "observers" that are called in response
 * to events in a schema model.  The platform observers can make platform-
 * specific modifications to the schema model.
 * @author cmaeda
 *
 */
abstract class Platform 
{
	String name
		
	protected void saveNativeColumnType(Database db, Table table, Column col)
	{
		String nativeType = col.typeName
		if (nativeType == null) {
			return	// nothing to do (using standard type)
		}
		
		Map typeMap = col.nativeTypeMap
		if (typeMap == null) {
			col.nativeTypeMap = [:]
		}
		col.nativeTypeMap[name] = nativeType
		col.typeName = null
	}

	
	// table data captured
	abstract void gotTableModel(Database db, Table table)

	// table columns captured
	abstract void gotTableColumns(Database db, Table table)

	// table primary key captured
	abstract void gotTablePrimaryKeys(Database db, Table table)

	// table foreign keys captured
	abstract void gotTableForeignKeys(Database db, Table table)

	// table indices captured
	abstract void gotTableIndices(Database db, Table table)

	abstract boolean hasStandardType(Column col)
	
	def boolean fetchIndicesForView()
	{
		return false
	}
	
	/**
	 * Replaces a specific character sequence in the given text with the character sequence
	 * whose escaped version it is.
	 *
	 * @param text      The text
	 * @param unescaped The unescaped string, e.g. "'"
	 * @param escaped   The escaped version, e.g. "''"
	 * @return The resulting text
	 */
	public static String unescape(String text, String unescaped, String escaped)
	{
		String result = text;

		// we need special handling if the single quote is escaped via a double single quote
		if (result != null)
		{
			if (escaped.equals("''"))
			{
				if ((result.length() > 2) && result.startsWith("'") && result.endsWith("'"))
				{
					result = "'" + result.substring(1, result.length() - 1).replace(escaped, unescaped) + "'"
				}
				else
				{
					result = result.replace(escaped, unescaped)
				}
			}
			else
			{
				result = result.replace(escaped, unescaped)
			}
		}
		return result;
	}
	
	//
	// DDL Generation
	//

	/**
	 * Returns DDL Statements to create table and indexes.
	 * Does not create foreign key constraints or any other constraints that depend on other tables.	
	 * @param db
	 * @param table
	 * @return
	 */
	abstract public List<String> generateTableDDL(Database db, Table table);
	
	abstract public List<String> generateTableForeignKeyConstraints(Database db, Table table);
	
	abstract public String generateDDLTypeName(int typeCode)
	
	abstract public String generateDDLEndOfLine()
	
}
