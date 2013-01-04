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
	public FieldSchema toFieldSchema(boolean fillInNullAliases) throws FrontendException {
		Schema s = new Schema(schema.toFieldSchema(fillInNullAliases)); //TODO make this match what Utils.getSchemaFromString does for b{(x:int)}
		return new FieldSchema(getAlias(), s, getDataType());
	}
}
