package org.apache.pig.impl.logicalLayer.schema.helper;

import java.util.List;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.NumValCarrier;

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
		return toSchema(true);
	}

	public Schema toSchema(boolean fillInNullAliases) {
		Schema s = new Schema();
		for (ColumnSchema column : columns) {
			try {
				s.add(column.toFieldSchema(fillInNullAliases));
			} catch (FrontendException e) {
				throw new RuntimeException(e);
			}
		}
		if (fillInNullAliases) {
			fixSchemaAliases(s);
		}
		return s;
	}

	public static void fixSchemaAliases(Schema s) {
		NumValCarrier nvc = new NumValCarrier();
		for (FieldSchema fs : s.getFields()) {
			if (fs.alias == null) {
				fs.alias = nvc.makeNameFromDataType(fs.type);
			}
		}
	}

	public static RelationSchema fromSchema(Schema schema) {
		RelationSchema rs = new RelationSchema();
		for (FieldSchema fs : schema.getFields()) {
			rs.add(fromFieldSchema(fs));
		}
		return rs;
	}

	public static ColumnSchema fromFieldSchema(FieldSchema fieldSchema) {
		switch (fieldSchema.type) {
		case DataType.BAG: {
      Schema innerSchema = fieldSchema.schema;
      if (innerSchema != null) {
        try {
          return new BagSchema(fieldSchema.alias, (TupleSchema)fromFieldSchema(fieldSchema.schema.getField(0)));
        } catch (FrontendException e) {
          throw new RuntimeException(e);
        }
      } else {
        return new BagSchema(fieldSchema.alias);
      }
		}
		case DataType.MAP: {
		  Schema innerSchema = fieldSchema.schema;
		  if (innerSchema != null) {
		    try {
          return new MapSchema(fieldSchema.alias, (TupleSchema)fromFieldSchema(fieldSchema.schema.getField(0)));
        } catch (FrontendException e) {
          throw new RuntimeException(e);
        }
		  } else {
		    return new MapSchema(fieldSchema.alias);
		  }
		}
		case DataType.TUPLE:
		  Schema innerSchema = fieldSchema.schema;
		  if (innerSchema != null) {
  		  ColumnSchema[] columns = new ColumnSchema[innerSchema.size()];
  		  int idx = 0;
  		  for (FieldSchema fs : innerSchema.getFields()) {
  		    columns[idx++] = fromFieldSchema(fs);
  		  }
  		  return new TupleSchema(fieldSchema.alias, columns);
		  } else {
		    return new TupleSchema(fieldSchema.alias);
		  }
		default:
			return new PrimitiveSchema(fieldSchema.alias, fieldSchema.type);
		}
	}
}