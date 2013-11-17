package gddlutils.main

import gddlutils.model.Database
import gddlutils.model.ModelReader
import gddlutils.model.ModelWriter

class ReadModel {

	static main(args) {
		String filename = args[0]
		File file = new File(filename)
		ModelReader mr = new ModelReader()
		mr.verbose = true
		Database db = mr.readModel(file)
		
		// write to new file so we can check work
		String outfile = args[1]
		ModelWriter.writeModel(db, outfile)					
	}

}
