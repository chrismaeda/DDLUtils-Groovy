package gddlutils.model

class ForeignKey
{
	// name of foreign key constraint
	String name
	
	// target table
	Table foreignTable
	String foreignTableName
	
	Table localTable
	String localTableName
	
	List<ColumnReference> references
	
	def void addRefence(ColumnReference ref) 
	{
		if (references == null) {
			references = []
		}
		references.add(ref)
	}
}
