package gddlutils.main

import gddlutils.model.Database
import gddlutils.model.ModelReader
import gddlutils.platform.SchemaDDLWriter
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

		// read model		
		File file = new File(modelfile)
		ModelReader mr = new ModelReader()
		mr.verbose = true
		Database db = mr.readModel(file)

		// write ddl
		OraclePlatform ora = new OraclePlatform();
		SchemaDDLWriter ddl = new SchemaDDLWriter()
		ddl.db = db
		ddl.platform = ora
		ddl.computeDDL()
		ddl.writeDDL(ddlfile)
		
	}

}
