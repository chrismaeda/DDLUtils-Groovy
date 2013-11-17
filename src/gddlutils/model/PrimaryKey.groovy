package gddlutils.model

class PrimaryKey
{
	String name
	List<IndexColumn> columns
	
	def void addColumn(IndexColumn idxCol)
	{
		if (columns == null) {
			columns = []
		}
		columns.add(idxCol)
	}
}
