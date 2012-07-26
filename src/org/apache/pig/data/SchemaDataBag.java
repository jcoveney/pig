package org.apache.pig.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.SpillableColumn.SpillableColumnIterator;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class SchemaDataBag implements DataBag {
    private static final long serialVersionUID = 1L;
    private long size = 0L;
    private SpillableColumn[] columns;
    private int schemaTupleId;
    private SchemaTupleFactory stf;

    /**
     * Note that spilling the SchemaDataBag itself will not be registered with the SpillableMemoryManager.
     * The columns that the implementation uses will independently be spillable, and will be registered with
     * the memory manager.
     */
    @Override
    public long spill() {
        return 0L;
    }

    @Override
    public long getMemorySize() {
        long memorySize = 8;
        for (SpillableColumn sc : getColumns()) {
            memorySize += sc.getMemorySize();
        }
        return memorySize;
    }

    public SchemaDataBag(int schemaId) {
        this(SchemaTupleFactory.getInstance(schemaId));
    }

    public SchemaDataBag(SchemaTupleFactory stf) {
        this.stf = stf;
        SchemaTuple<?> st = stf.newTuple();
        schemaTupleId = st.getSchemaTupleIdentifier();
        int columnCount = st.size();
        Schema schema = st.getSchema();
        columns = new SpillableColumn[columnCount];
        int i = 0;
        for (FieldSchema fs : schema.getFields()) {
            switch (fs.type) {
            case (DataType.INTEGER):
                columns[i++] = BagFactory.getIntSpillableColumn();
                break;
            default:
                throw new RuntimeException("SchemaDataBag not supported for given Schema ["+schema+"]");
            }
        }
    }

    public int getSchemaTupleBagIdentifier() {
        return schemaTupleId;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        throw new RuntimeException("readFields(DataInput) not implemented yet!");
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int id = getSchemaTupleBagIdentifier();

        if (size < BinInterSedes.UNSIGNED_BYTE_MAX) {
            if (id < BinInterSedes.UNSIGNED_BYTE_MAX) {
                out.writeByte(BinInterSedes.SCHEMA_TUPLE_BAG_BYTE_ID_BYTE_SIZE);
                out.writeByte(id);
            } else if (id < BinInterSedes.UNSIGNED_SHORT_MAX) {
                out.writeByte(BinInterSedes.SCHEMA_TUPLE_BAG_SHORT_ID_BYTE_SIZE);
                out.writeShort(id);

            } else {
                out.writeByte(BinInterSedes.SCHEMA_TUPLE_BAG_INT_ID_BYTE_SIZE);
                out.writeInt(id);
            }
            out.writeByte((int)size);
        } else if (size < BinInterSedes.UNSIGNED_SHORT_MAX) {
            if (id < BinInterSedes.UNSIGNED_BYTE_MAX) {
                out.writeByte(BinInterSedes.SCHEMA_TUPLE_BAG_BYTE_ID_SHORT_SIZE);
                out.writeByte(id);
            } else if (id < BinInterSedes.UNSIGNED_SHORT_MAX) {
                out.writeByte(BinInterSedes.SCHEMA_TUPLE_BAG_SHORT_ID_SHORT_SIZE);
                out.writeShort(id);
            } else {
                out.writeByte(BinInterSedes.SCHEMA_TUPLE_BAG_INT_ID_SHORT_SIZE);
                out.writeInt(id);
            }
            out.writeShort((int)size);
        } else if (size < BinInterSedes.UNSIGNED_INT_MAX){
            if (id < BinInterSedes.UNSIGNED_BYTE_MAX) {
                out.writeByte(BinInterSedes.SCHEMA_TUPLE_BAG_BYTE_ID_INT_SIZE);
                out.writeByte(id);
            } else if (id < BinInterSedes.UNSIGNED_SHORT_MAX) {
                out.writeByte(BinInterSedes.SCHEMA_TUPLE_BAG_SHORT_ID_INT_SIZE);
                out.writeByte(id);
            } else {
                out.writeByte(BinInterSedes.SCHEMA_TUPLE_BAG_INT_ID_INT_SIZE);
                out.writeInt(id);
            }
            out.writeInt((int)size);
        } else {
            if (id < BinInterSedes.UNSIGNED_BYTE_MAX) {
                out.writeByte(BinInterSedes.SCHEMA_TUPLE_BAG_BYTE_ID_LONG_SIZE);
                out.writeByte(id);
            } else if (id < BinInterSedes.UNSIGNED_SHORT_MAX) {
                out.writeByte(BinInterSedes.SCHEMA_TUPLE_BAG_SHORT_ID_LONG_SIZE);
                out.writeShort(id);
            } else {
                out.writeByte(BinInterSedes.SCHEMA_TUPLE_BAG_INT_ID_LONG_SIZE);
                out.writeInt(id);
            }
            out.writeLong(size);
        }
        for (SpillableColumn sc : getColumns()) {
            sc.writeData(out);
        }
    }

    @Override
    public int compareTo(Object o) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public boolean isSorted() {
        return false;
    }

    @Override
    public boolean isDistinct() {
        return false;
    }

    @Override
    public Iterator<Tuple> iterator() {
        return new Iterator<Tuple>() {
            SpillableColumnIterator[] sci;

            @Override
            public boolean hasNext() {
                lazyInit();
                boolean hasNext = sci[0].hasNext();
                if (!hasNext) {
                    for (SpillableColumnIterator col : sci) {
                        col.finish();
                    }
                }
                return hasNext;
            }

            @Override
            public Tuple next() {
                lazyInit();
                SchemaTuple<?> st = stf.newTuple();
                int i = 0;
                for (SpillableColumnIterator col : sci) {
                    try {
                        col.setTuplePositionWithNext(st, i++);
                    } catch (ExecException e) {
                        throw new RuntimeException(e); //TODO do more
                    }
                }
                return st;
            }

            private void lazyInit() {
                if (sci == null) {
                    sci = new SpillableColumnIterator[columns.length];
                    for (int i = 0; i < sci.length; i++) {
                        sci[i] = columns[i].iterator();
                    }
                }
            }

            @Override
            public void remove() {
                throw new RuntimeException("remove() not implemented");
            }
        };
    }

    @Override
    public void add(Tuple t) {
        add(t, true);
    }

    public void add(Tuple t, boolean checkType) {
        if (checkType && t instanceof TypeAwareTuple) {
            add((TypeAwareTuple)t, false);
        } else {
            size++;
            int i = 0;
            for (SpillableColumn sc : columns) {
                try {
                    sc.getFromPosition(t, i++);
                } catch (ExecException e) {
                    throw new RuntimeException(e); //TODO do more
                }
            }
        }
    }

    public void add(TypeAwareTuple t) {
        size++;
        int i = 0;
        for (SpillableColumn sc : columns) {
            try {
                sc.getFromPosition(t, i++);
            } catch (ExecException e) {
                throw new RuntimeException(e); //TODO do more
            }
        }
    }

    @Override
    public void addAll(DataBag b) {
        for (Tuple t : b) {
            add(t);
        }
    }

    public SpillableColumn[] getColumns() {
        return columns;
    }

    public void setColumns(SpillableColumn[] columns) {
        throw new RuntimeException("Need to implement setColumns");
    }

    @Override
    public void clear() {
        for (SpillableColumn sc : getColumns()) {
            sc.clear();
        }
    }

    @Override
    public void markStale(boolean stale) {}
}
