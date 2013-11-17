package gddlutils.platform

import gddlutils.model.Database
import gddlutils.model.Table

class SchemaDDLWriter
{
	boolean verbose
	Database db
	Platform platform
	List<String> pass1
	List<String> pass2
	
	public void computeDDL()
	{
		pass1 = []
		pass2 = []
		
		// compute pass 1 
		for (tentry in db.tables.entrySet()) {
			Table table = tentry.value
			if (table.type.equalsIgnoreCase("TABLE")) {
				List<String> tabddl = platform.generateTableDDL(db, table)
				pass1.addAll(tabddl)
			}
		}

		// compute pass 2 
		for (tentry in db.tables.entrySet()) {
			Table table = tentry.value
			if (table.type.equalsIgnoreCase("TABLE")) {
				List<String> tabddl = platform.generateTableForeignKeyConstraints(db, table)
				pass2.addAll(tabddl)
			}
		}
	}
	
	public void writeDDL(PrintWriter pw)
	{
		for (String stmt in pass1) {
			pw.print(stmt)
			pw.println(platform.generateDDLEndOfLine())
		}
		
		for (String stmt in pass2) {
			pw.print(stmt)
			pw.println(platform.generateDDLEndOfLine())
		}
	}
	
	public void writeDDL(File file)
	{
		FileWriter fw = null
		try {
			fw = new FileWriter(file)
			writeDDL(new PrintWriter(fw))
		}
		finally {
			if (fw != null) {
				try { fw.close() } catch (Exception x) {}
			}
		}
	}

	public void writeDDL(String filename)
	{
		File file = new File(filename)
		writeDDL(file)
	}

}
