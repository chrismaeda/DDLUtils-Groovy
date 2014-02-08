package gddlutils.main

import gddlutils.model.Database
import gddlutils.model.ModelReader
import gddlutils.platform.SchemaDDLWriter
import gddlutils.platform.mysql.MySqlPlatform;
import gddlutils.platform.oracle.OraclePlatform

class GenDDL {

	static main(args) 
	{
		if (args.length < 2) {
			println "usage: modelfile ddlfile"
			System.exit(1)
		}
		
		String modelfile = args[0]
		String ddlfile = args[1]

		/*String modelfile = "C:/Users/S/Desktop/schemaModelFile.xml"
		String ddlfile = "C:/Users/S/Desktop/dbDefFile.sql"*/

		// read model		
		File file = new File(modelfile)
		ModelReader mr = new ModelReader()
		mr.verbose = true
		Database db = mr.readModel(file)

		// write ddl
		OraclePlatform ora = new OraclePlatform();
		MySqlPlatform mysqlPlatform = new MySqlPlatform()

		SchemaDDLWriter ddl = new SchemaDDLWriter()

		ddl.db = db
		// ddl.platform = ora
		ddl.platform = mysqlPlatform

		ddl.computeDDL()

		ddl.writeDDL(ddlfile)
	}

}
