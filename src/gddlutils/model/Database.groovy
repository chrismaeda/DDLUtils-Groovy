package gddlutils.model

import java.util.Map;

/**
 * Represents a schema model
 * @author cmaeda
 *
 */
class Database 
{
	String name
	String catalog
	String schema
	
	TreeMap<String, Table> tables
	
	public void addTable(Table tab)
	{
		if (tables == null) {
			tables = new TreeMap<String, Table>();
		}
		tables[tab.name] = tab
	}

	// used by ModelWriter	
	public Map schemaModelAttrMap()
	{
		def attrs = [name:name]
		
		if (catalog) {
			attrs["catalog"] = catalog
		}
		if (schema) {
			attrs["schema"] = schema
		}
		
		return attrs
	}

}
