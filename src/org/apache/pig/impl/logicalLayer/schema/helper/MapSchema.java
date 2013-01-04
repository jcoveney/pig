package org.apache.pig.impl.logicalLayer.schema.helper;

import java.util.List;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import com.google.common.collect.Lists;

public class MapSchema extends ColumnSchema {
	private List<ColumnSchema> valueColumns = null;

	public MapSchema(String alias, TupleSchema ts) {
	  this(alias, ts.getColumns());
	}

	public MapSchema(String alias) {
	  this(alias, (ColumnSchema[])null);
	}

	public MapSchema(String alias, ColumnSchema... valueColumns) {
		super(alias, DataType.MAP);
		if (valueColumns != null) {
		  this.valueColumns = Lists.newArrayList(valueColumns);
		}
	}

	@Override
	public FieldSchema toFieldSchema(boolean fillInNullAliases) throws FrontendException {
		Schema s = null;
		if (valueColumns != null) {
  		s = new Schema();
  		for (ColumnSchema column : valueColumns) {
  			s.add(column.toFieldSchema(fillInNullAliases));
  		}
  		if (fillInNullAliases) {
  			RelationSchema.fixSchemaAliases(s);
  		}
  		s = new Schema(new FieldSchema(null, s, DataType.TUPLE));
		}
		return new FieldSchema(getAlias(), s, getDataType());
	}

	public ColumnSchema[] getValueColumns() {
    return valueColumns.toArray(new ColumnSchema[valueColumns.size()]);
	}
}