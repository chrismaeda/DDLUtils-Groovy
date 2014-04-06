package gddlutils.model

import java.sql.Types

/**
 * Implements 2-way mapping between JDBC type codes and names
 * @see java.sql.Types
 * @author cmaeda
 *
 */

enum TypeCat
{
	TEXTUAL,
	NUMERIC,
	DATETIME,
	BINARY,
	OPAQUE
}

class TypeMap
{
	private static TypeName = [:]
	private static TypeCode = [:]
	private static TypeCatMap = [:]

	static {
		mapType(Types.CHAR, "CHAR", TypeCat.TEXTUAL)
		mapType(Types.NCHAR, "NCHAR", TypeCat.TEXTUAL)
		mapType(Types.VARCHAR, "VARCHAR", TypeCat.TEXTUAL)
		mapType(Types.NVARCHAR, "NVARCHAR", TypeCat.TEXTUAL)
		mapType(Types.LONGVARCHAR, "LONGVARCHAR", TypeCat.TEXTUAL)
		mapType(Types.LONGNVARCHAR, "LONGNVARCHAR", TypeCat.TEXTUAL)
		mapType(Types.CLOB, "CLOB", TypeCat.TEXTUAL)
		mapType(Types.NCLOB, "NCLOB", TypeCat.TEXTUAL)

		mapType(Types.BIT, "BIT", TypeCat.NUMERIC)
		mapType(Types.TINYINT, "TINYINT", TypeCat.NUMERIC)
		mapType(Types.SMALLINT, "SMALLINT", TypeCat.NUMERIC)
		mapType(Types.INTEGER, "INTEGER", TypeCat.NUMERIC)
		mapType(Types.BIGINT, "BIGINT", TypeCat.NUMERIC)
		mapType(Types.NUMERIC, "NUMERIC", TypeCat.NUMERIC)
		mapType(Types.DECIMAL, "DECIMAL", TypeCat.NUMERIC)
		mapType(Types.REAL, "REAL", TypeCat.NUMERIC)
		mapType(Types.DOUBLE, "DOUBLE", TypeCat.NUMERIC)

		mapType(Types.BINARY, "BINARY", TypeCat.BINARY)
		mapType(Types.VARBINARY, "VARBINARY", TypeCat.BINARY)
		mapType(Types.LONGVARBINARY, "LONGVARBINARY", TypeCat.BINARY)
		mapType(Types.BLOB, "BLOB", TypeCat.BINARY)
		mapType(Types.ROWID, "ROWID", TypeCat.BINARY)
		
		mapType(Types.DATE, "DATE", TypeCat.DATETIME)
		mapType(Types.TIME, "TIME", TypeCat.DATETIME)
		mapType(Types.TIMESTAMP, "TIMESTAMP", TypeCat.DATETIME)

		mapType(Types.ROWID, "ROWID", TypeCat.OPAQUE)
	}

	static void mapType(int typeCode, String typeName, TypeCat cat)
	{
		TypeName[typeCode] = typeName
		TypeCode[typeName] = typeCode
		TypeCatMap[typeCode] = cat
	}

	static String getTypeName(int typeCode)
	{
		return TypeName[typeCode]
	}

	static TypeCat getTypeCategory(int typeCode)
	{
		return TypeCatMap[typeCode]
	}

	static int getTypeCode(String typeName)
	{
		return TypeCode[typeName]
	}

	static boolean isTextType(int typeCode)
	{
		return TypeCatMap[typeCode] == TypeCat.TEXTUAL
	}
	static boolean isNumericType(int typeCode)
	{
		return TypeCatMap[typeCode] == TypeCat.NUMERIC
	}
	static boolean isDateTimeType(int typeCode)
	{
		return TypeCatMap[typeCode] == TypeCat.DATETIME
	}
	static boolean isBinaryType(int typeCode)
	{
		return TypeCatMap[typeCode] == TypeCat.BINARY
	}
}
