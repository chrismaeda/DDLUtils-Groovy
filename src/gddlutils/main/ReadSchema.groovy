package gddlutils.main

import gddlutils.model.Database
import gddlutils.model.Table
import gddlutils.model.ModelWriter
import gddlutils.platform.JdbcModelReader
import gddlutils.platform.oracle.OraclePlatform
import groovy.xml.MarkupBuilder
import java.sql.Connection;
import java.sql.DriverManager;

class ReadSchema 
{
	static main(String[] args) 
	{
		if (args.length < 2)
		{
			println "Usage: jdbc.properties schema.xml"
			println "jdbc.properties file should have these properties:"
			println "jdbc.driver - class name of driver, e.g. com.mysql.jdbc.Driver"
			println "jdbc.url - url of source database"
			println "jdbc.user - jdbc user name"
			println "jdbc.pass - jdbc password"
			println "jdbc.schema - jdbc schema name to reverse engineer"
			System.exit(1);
		}
	
		String propFile = args[0]
		String modelFile = args[1]
		
		// read jdbc properties
		Properties jdbcProps = new Properties()
		FileInputStream fis = null
		try {
			fis = new FileInputStream(propFile)
			jdbcProps.load(fis)
		}
		catch (Exception x) {
			x.printStackTrace()
			System.exit(1);
		}
		finally {
			if (fis != null) {
				try { fis.close() } catch (Exception x) {}
			}
		}
			
		// extract jdbc info from properties file
		String driver = jdbcProps.getProperty("jdbc.driver");
		String url = jdbcProps.getProperty("jdbc.url");
		String user = jdbcProps.getProperty("jdbc.user");
		String pass = jdbcProps.getProperty("jdbc.pass");
		String schema = jdbcProps.getProperty("jdbc.schema");

		// open db connection
		Connection conn = null;
		try {
			Class.forName(driver).newInstance();
			conn = DriverManager.getConnection(url, user, pass);
		}
		catch (Exception x) {
			x.printStackTrace();
			System.exit(1);
		}

		Database db = new Database()
		db.name = schema

		JdbcModelReader jreader = new JdbcModelReader()
		jreader.conn = conn
		jreader.dbmeta = conn.metaData
		jreader.db = db
		jreader.platform = new OraclePlatform()
		
		// read tables
		TreeMap<String,Table> tables = jreader.getTables(null, schema, null)
		
		// export tables as xml
		ModelWriter.writeModel(db, modelFile)
	}

}
