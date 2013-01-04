package org.apache.pig.impl.logicalLayer.schema.helper;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class BagSchema extends ColumnSchema {
	private TupleSchema schema;
	
	public BagSchema(String alias, TupleSchema schema) {
		super(alias, DataType.BAG);
		this.schema = schema;
	}
	
	public BagSchema(TupleSchema schema) {
		this(null, schema);
	}
	
	@Override
	public FieldSchema toFieldSchema() throws FrontendException {
		Schema s = new Schema(schema.toFieldSchema());
		return new FieldSchema(getAlias(), s, getDataType());
	}
}
