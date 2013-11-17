package gddlutils.model

/**
 * Represents a reference between a column in the local table and a column in another table
 * @author cmaeda
 */
class ColumnReference 
{
	// sequence value within key
	int sequenceValue
	
	// column in local table
	Column localColumn
	String localColumnName
	
	// column in foreign table
	Column foreignColumn
	String foreignColumnName
}
