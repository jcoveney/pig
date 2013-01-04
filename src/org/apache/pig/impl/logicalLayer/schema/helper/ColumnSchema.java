package org.apache.pig.impl.logicalLayer.schema.helper;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public abstract class ColumnSchema {
	private String alias;
	private byte dataType;
	
	public ColumnSchema(String alias, byte dataType) {
		this.alias = alias;
		this.dataType = dataType;
	}
	
	public String getAlias() {
		return alias;
	}
	
	public byte getDataType() {
		return dataType;
	}
	
	public abstract ColumnSchema fromSchema();
	
	public FieldSchema toFieldSchema() throws FrontendException {
		return toFieldSchema(false);
	}
	
	public abstract FieldSchema toFieldSchema(boolean fillInNullAliases) throws FrontendException;
}
