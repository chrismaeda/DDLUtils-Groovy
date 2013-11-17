package gddlutils.model

import java.util.jar.Attributes.Name;

/**
 * Reads a schema model from an XML file
 * @author cmaeda
 *
 */
public class ModelReader 
{
	boolean verbose
	
	public Database readModel(File file)
	{
		FileInputStream fis = null
		Database result = null
		try {
			fis = new FileInputStream(file)
			result = readModel(fis)
			return result			
		}
		finally {
			if (fis != null) {
				try { fis.close() } catch (Exception x) {}
			}
		}
	}
	
	public Database readModel(InputStream is)
	{
		XmlSlurper slurper = new XmlSlurper()
		def xmldb = slurper.parse(is)
		
		Database db = readSchemaFromXml(xmldb) 
		return db
	}

	public Database readSchemaFromXml(xmldb)
	{
		Database db = new Database()
		db.name = xmldb.@name
		db.catalog = xmldb.@catalog
		db.schema = xmldb.@schema
		db.tables = []
		
		for (xmltable in xmldb.table) {
			Table tab = readTableFromXml(xmltable)
			db.addTable(tab)
		}
		
		// second pass for foreign keys
		for (entry in db.tables.entrySet) {
			String tabname = entry.key
			String table = entry.value
			resolveForeignKeyReferences(table, db)			
		}
		
		return db
	}
	
	public Table readTableFromXml(xmltable)
	{
		Table tab = new Table()
		tab.name = xmltable.@name
		tab.type = xmltable.@type
		if (verbose) {
			println "READ " + tab.type + " " + tab.name
		}
		
		// read columns
		List<Column> columns = []
		for (xmlcol in xmltable.column) {			
			Column column = readColumnFromXml(xmlcol)
			columns.add(column)
		}
		tab.columns = columns
		
		// read primary key
		def xmlpks = xmltable.primaryKey
		// verify only one pk per table???
		for (xmlpk in xmlpks) {
			// expect this to only happen once per table			
			PrimaryKey pk = readPrimaryKeyFromXml(xmlpk, tab)
			tab.primaryKey = pk
		}
		
		// read foreign keys
		def xmlfks = xmltable.foreignKey
		for (xmlfk in xmlfks) {
			ForeignKey fk = readForeignKeyFromXml(xmlfk, tab)
			tab.addForeignKey(fk)
		}
		
		// read indexes
		def xmlidxs = xmltable.index
		for (xmlidx in xmlidxs) {
			Index idx = readIndexFromXml(xmlidx, tab)
			tab.addIndex(idx)
		}
		
		return tab
	}
	
	public Column readColumnFromXml(xmlcol)
	{
		Column col = new Column()
		
		// read string instance vars
		col.name = xmlcol.@name
		if (verbose) {
			println "\tColumn " + col.name
		}

		// read non-string instance vars
		String aval = null

		aval = xmlcol.@type
		col.typeCode = TypeMap.getTypeCode(aval)
		
		
		// size
		aval = xmlcol.@size
		// Note: aval is true if non-null and non-empty
		if (aval) {
			col.size = Integer.parseInt(aval)
		}
		// scale
		aval = xmlcol.@scale
		if (aval) {
			col.scale = Integer.parseInt(aval)
		}
		
		// primary key
		aval = xmlcol.@primaryKey
		if (aval) {
			col.primaryKey = Boolean.parseBoolean(aval)
		}
		// required
		aval = xmlcol.@required
		if (aval) {
			col.required = Boolean.parseBoolean(aval)
		}
		// autoincrement
		aval = xmlcol.@autoIncrement
		if (aval) {
			col.required = Boolean.parseBoolean(aval)
		}
		// default
		aval = xmlcol.@default
		if (aval) {
			col.defaultVal = aval
		}
		
		//
		// native type map
		//
		def xmlntm = xmlcol.nativeTypeMap
		if (xmlntm != null)
		{
			def entries = xmlntm.entry
			for (xmlentry in entries)
			{
				String platform = xmlentry.@platform
				String ntype = xmlentry.@type
				col.addNativeType(platform, ntype)
			}
		}
		
		return col
	}

	public PrimaryKey readPrimaryKeyFromXml(xmlpk, Table table)
	{
		PrimaryKey pk = new PrimaryKey()
		pk.name = xmlpk.@name
		if (verbose) {
			println "\tPRIMARY KEY " + pk.name
		}

		// read columns
		def idxcols = xmlpk.indexColumn
		for (idxcol in idxcols) {
			IndexColumn idx = new IndexColumn()
			idx.columnName = idxcol.@name
			// lookup column from name
			Column column = table.getColumnByName(idx.columnName)
			assert column != null, "Invalid Column " + idx.columnName
			idx.column = column
			pk.addColumn(idx) 
		}
		
		return pk
	}
	
	// Note: foreign keys are read in two passes.
	// In the first pass, we read the table and column names
	// In the second pass, we resolve the names to table and column objects
	// We cannot do this in one pass since all the tables have not been read
	// yet in the first pass.
	public ForeignKey readForeignKeyFromXml(xmlfk, Table table)
	{
		ForeignKey fk = new ForeignKey()
		fk.name = xmlfk.@name
		if (verbose) {
			println "\tFOREIGN KEY " + fk.name
		}

		fk.localTableName = table.name
		fk.localTable = table
		fk.foreignTableName = xmlfk.@foreignTable
		// note: populate fk.foreignTable in second pass
		
		// read reference elements
		def refcols = xmlfk.reference
		int key_seq = 1
		for (refcol in refcols) {
			ColumnReference colref = new ColumnReference()
			colref.sequenceValue = key_seq
			colref.localColumnName = refcol.@local
			colref.foreignColumnName = refcol.@foreign
			// note: populate column objects in second pass
			fk.addRefence(colref)
			key_seq++
		}
		
		return fk
	}
	
	// pass 2, called once all tables have been read
	public void resolveForeignKeyReferences(Table table, Database db)
	{
		for (entry in table.foreignKeys.entrySet) {
			String fkName = entry.key
			ForeignKey fk = entry.value
			
			// lookup foreign table
			String foreignTableName = fk.foreignTableName
			Table foreignTable = db.tables[foreignTableName]
			assert foreignTable != null, "FK " + fkName + ": cannot resolve foreign table: " + foreignTableName
			fk.foreignTable = foreignTable
			
			// resolve columns
			for (ColumnReference colref in fk.references) {
				Column localColumn = table.getColumnByName(colref.localColumnName)
				assert localColumn != null, "FK " + fkName + ": invalid local column: " + colref.localColumnName
				Column foreignColumn = foreignTable.getColumnByName(colref.foreignColumnName)
				assert foreignColumn != null, "FK " + fkName + ": invalid foreign column: " + colref.foreignColumnName				
			}			
		}	
	}
	
	public Index readIndexFromXml(xmlidx, Table table)
	{
		Index idx = new Index()
		idx.name = xmlidx.@name
		if (verbose) {
			println "\tINDEX " + idx.name
		}

		String aval = null
		aval = xmlidx.@unique
		assert aval, "Index has no unique attribute"
		idx.unique = Boolean.parseBoolean(aval)
		
		// read index column elements
		def idxcols = xmlidx.indexColumn
		int key_seq = 1
		for (idxcol in idxcols) {
			IndexColumn colref = new IndexColumn()
			colref.ordinalPosition = key_seq
			colref.columnName = idxcol.@name
			Column column = table.getColumnByName(colref.columnName)
			assert column != null, "Invalid Index Column " + colref.columnName
			colref.column = column
			idx.addColumn(colref)
			key_seq++
		}
		
		return idx
	}
}
