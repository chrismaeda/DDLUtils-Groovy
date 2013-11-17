package gddlutils.model

import gddlutils.platform.Platform
import java.sql.Types

/**
 * Represents a single column definition
 * @author cmaeda
 *
 */
class Column
{
	String name
	String description
	boolean primaryKey
	boolean autoIncrement
	boolean required
	
	// type info
	int typeCode		// java.sql.Types
	String typeName
	Integer size
	Integer scale
	Map nativeTypeMap	// allow type override
	
	// default
	String defaultVal	// default expression
	
	public void addNativeType(String platform, String ntype)
	{
		if (nativeTypeMap == null) {
			nativeTypeMap = [:]
		}
		nativeTypeMap[platform] = ntype
	}
	
	public Map schemaModelAttrMap()
	{
		String typnam = TypeMap.getTypeName(typeCode)
		assert typnam, "No type name for code $typeCode"
		 
		def attrs = [
			name:name, 
			type:typnam 
			]

		if (size != null) {
			attrs["size"] = size
		}
		if (scale != null && scale > 0) {
			attrs["scale"] = scale
		}

		if (typeName != null) {
			attrs["nativeType"] = typeName
		}
		// only print primaryKey if true
		if (primaryKey) {
			attrs["primaryKey"] = primaryKey
		}
		// only print required if true
		if (required) {
			attrs["required"] = required
		}
		// only print autoincrement if true
		if (autoIncrement) {
			attrs["autoIncrement"] = autoIncrement
		}
		if (defaultVal != null) {
			attrs["default"] = defaultVal
		}
		
		return attrs 
	}	
	
	//
	// DDL GENERATION
	//
	
	public String getTypeNameForDDL(Platform platform)
	{
		String dbtype = platform.name
		
		// see if there is a native type override
		if (nativeTypeMap != null) {
			String override = nativeTypeMap[dbtype]
			if (override) {
				return override
			}
		}
		// default
		String tname = platform.generateDDLTypeName(typeCode)
		assert tname != null, "Unknown type name for code $typeCode"
		return tname
	}
	
	public String getTypeSizeForDDL(Platform platform)
	{
		String result = ""
		if (size != null) {
			result = "(" + size
			if (scale != null) {
				result += "," + scale	
			}
			result += ")"
		}
		return result
	}
}
