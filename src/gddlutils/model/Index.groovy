package gddlutils.model

class Index
{
	String name
	boolean unique	
	List<IndexColumn> columns
	
	def void addColumn(IndexColumn idxCol)
	{
		if (columns == null) {
			columns = []
		}
		columns.add(idxCol)
	}
}
