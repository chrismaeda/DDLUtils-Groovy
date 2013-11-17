package gddlutils.model

/**
 * Represents a single table definition.
 * @author cmaeda
 *
 */
class Table 
{
	String name
	String catalog
	String schema
	String description
	String type
	
	PrimaryKey primaryKey
	List<Column> columns
	TreeMap<String,ForeignKey> foreignKeys
	TreeMap<String,Index> indices
	
	public Column getColumnByName(String colname)
	{
		if (colname == null) {
			throw new NullPointerException()
		}
		if (columns == null) {
			return null
		}

		for (Column col in columns) {
			if (colname.equalsIgnoreCase(col.name)) {
				return col
			}
		}
		return null
	}
	
	public void addForeignKey(ForeignKey fk)
	{
		if (foreignKeys == null) {
			foreignKeys = new TreeMap<String,ForeignKey>()
		}
		String fkname = fk.name
		foreignKeys[fkname] = fk
	}
	
	public void addIndex(Index idx)
	{
		if (indices == null) {
			indices = new TreeMap<String,Index>()			
		}
		String idxname = idx.name
		indices[idxname] = idx
	}
}
