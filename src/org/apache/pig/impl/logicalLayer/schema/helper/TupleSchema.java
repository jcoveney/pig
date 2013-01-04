package org.apache.pig.impl.logicalLayer.schema.helper;

import java.util.List;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import com.google.common.collect.Lists;

public class TupleSchema extends ColumnSchema {
	private List<ColumnSchema> columns;
	
	public TupleSchema(String alias, ColumnSchema... columns) {
		super(alias, DataType.TUPLE);
		this.columns = Lists.newArrayList(columns);
	}
	
	public TupleSchema(ColumnSchema... columns) {
		this(null, columns);
	}
	
	public void add(ColumnSchema column) {
		columns.add(column);
	}

	@Override
	public FieldSchema toFieldSchema(boolean fillInNullAliases) throws FrontendException {
		Schema s = new Schema();
		for (ColumnSchema column : columns) {
			s.add(column.toFieldSchema(fillInNullAliases));
		}
		if (fillInNullAliases) {
			RelationSchema.fixSchemaAliases(s);
		}
		return new FieldSchema(getAlias(), s, getDataType());
	}
	
	public ColumnSchema[] getColumns() {
		ColumnSchema[] columns = new ColumnSchema[this.columns.size()];
		return this.columns.toArray(columns);
	}
}
