package gddlutils.model

import groovy.xml.MarkupBuilder

/**
 * Write a model to an xml file
 * @author cmaeda
 *
 */
public class ModelWriter 
{
	public static writeModel(Database db, String filename)
	{
		FileWriter fw = null
		try {
			fw = new FileWriter(filename)
			ModelWriter.writeModel(db, new PrintWriter(fw))
		}
		finally {
			if (fw != null) {
				try { fw.close() } catch (Exception x) {}
			}
		}
	}
	
	public static writeModel(Database db, PrintWriter pw) 
	{
		MarkupBuilder b = new MarkupBuilder(pw)
		def dbattrs = db.schemaModelAttrMap()
		def dbxml = b.database(dbattrs) {
			for (tabentry in db.tables) {
				Table tab = tabentry.value
				table(name:tab.name, type:tab.type) {
					//
					// columns
					//
					for (Column col in tab.columns) {
						def colattrs = col.schemaModelAttrMap()
						column(colattrs) {
							if (col.nativeTypeMap != null) {
								nativeTypeMap {
									for (nativeType in col.nativeTypeMap) {
										entry(platform:nativeType.key, type:nativeType.value)
									}
								}								
							}
						}
					}
					//
					// primary key
					//
					if (tab.primaryKey != null) {
						PrimaryKey pk = tab.primaryKey
						primaryKey(name:pk.name) {
							for (IndexColumn idxCol in pk.columns) {
								indexColumn(name:idxCol.columnName)
							}
						}
					}
					// 
					// foreign keys
					//
					if (tab.foreignKeys != null) {
						for (fkentry in tab.foreignKeys) {
							ForeignKey fk = fkentry.value
							foreignKey(foreignTable:fk.foreignTableName, name: fk.name) {
								for (ColumnReference colRef in fk.references) {
									reference(local:colRef.localColumnName, 
										foreign:colRef.foreignColumnName)
								}
								
							}
						}
					}
					//
					// indexes
					//
					if (tab.indices != null) {
						for (idxentry in tab.indices) {
							Index idx = idxentry.value
							index(name:idx.name, unique:idx.unique) {
								for (IndexColumn idxCol in idx.columns) {
									indexColumn(name:idxCol.columnName)
								}
							}
						}
					}
				}
			}
		}		
	}

}
