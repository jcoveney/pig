package org.apache.pig.impl.logicalLayer.schema.helper;

import java.util.List;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import com.google.common.collect.Lists;

public class MapSchema extends ColumnSchema {
	private List<ColumnSchema> valueColumns;

	public MapSchema(String alias, ColumnSchema... valueColumns) {
		super(alias, DataType.MAP);
		this.valueColumns = Lists.newArrayList(valueColumns);
	}

	@Override
	public FieldSchema toFieldSchema(boolean fillInNullAliases) throws FrontendException {
		Schema s = new Schema();
		for (ColumnSchema column : valueColumns) {
			s.add(column.toFieldSchema(fillInNullAliases)); //TODO make this match what Utils.getSchemaFromString does
		}
		if (fillInNullAliases) {
			RelationSchema.fixSchemaAliases(s);
		}
		Schema s2 = new Schema(new FieldSchema(null, s, DataType.TUPLE)); 
		return new FieldSchema(getAlias(), s2, getDataType());
	}
}