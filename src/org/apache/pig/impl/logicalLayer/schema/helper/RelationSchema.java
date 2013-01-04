package org.apache.pig.impl.logicalLayer.schema.helper;

import java.util.List;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.collect.Lists;

public class RelationSchema {
	private List<ColumnSchema> columns;
	
	public RelationSchema() {
		columns = Lists.newArrayList();		
	}
	
	public RelationSchema(ColumnSchema... columns) {
		this.columns = Lists.newArrayList(columns);
	}
	
	public void add(ColumnSchema schema) {
		columns.add(schema);
	}
	
	public Schema toSchema() {
		Schema s = new Schema();
		for (ColumnSchema column : columns) {
			try {
				s.add(column.toFieldSchema());
			} catch (FrontendException e) {
				throw new RuntimeException(e);
			}
		}
		return s;
	}
}
