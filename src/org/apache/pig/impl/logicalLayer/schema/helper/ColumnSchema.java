package org.apache.pig.impl.logicalLayer.schema.helper;

import org.apache.pig.data.DataType;
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

	public FieldSchema toFieldSchema() throws FrontendException {
		return toFieldSchema(false);
	}

	public abstract FieldSchema toFieldSchema(boolean fillInNullAliases) throws FrontendException;

	public boolean isBag() {
	  return DataType.BAG == getDataType();
	}

	public boolean isTuple() {
	  return DataType.TUPLE == getDataType();
	}

	public boolean isMap() {
	  return DataType.MAP == getDataType();
	}

	public boolean isBoolean() {
	  return DataType.BOOLEAN == getDataType();
	}

	public boolean isInteger() {
	  return DataType.INTEGER == getDataType();
	}

	public boolean isLong() {
	  return DataType.LONG == getDataType();
	}

	public boolean isFloat() {
	  return DataType.FLOAT == getDataType();
	}

	public boolean isDouble() {
	  return DataType.DOUBLE == getDataType();
	}

	public boolean isChararray() {
	  return DataType.CHARARRAY == getDataType();
	}
}