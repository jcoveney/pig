package org.apache.pig.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

public abstract class SchemaDataBag<T extends SchemaTuple<T>> implements DataBag {
    private static final long serialVersionUID = 1L;
    private long size = 0L;

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

    public abstract boolean isSpecificSchemaTuple(Tuple t);
    public abstract int getSchemaTupleBagIdentifier();

    @Override
    public void readFields(DataInput in) throws IOException {
        throw new RuntimeException("readFields(DataInput) not used");
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
    public abstract Iterator<Tuple> iterator();

    @Override
    public void add(Tuple t) {
        add(t, true);
    }

    @SuppressWarnings("unchecked")
    public void add(Tuple t, boolean checkType) {
        if (checkType) {
            if (isSpecificSchemaTuple(t)) {
                addSpecific((T)t);
                return;
            } else if (t instanceof SchemaTuple<?>){
                add((SchemaTuple<?>)t, false);
                return;
            }
        }
        size++;
        generatedCodeAdd(t);
    }

    public abstract void generatedCodeAdd(Tuple t);

    public void add(SchemaTuple<?> t) {
        add(t, true);
    }

    @SuppressWarnings("unchecked")
    public void add(SchemaTuple<?> t, boolean checkType) {
        if (checkType && isSpecificSchemaTuple(t)) {
            addSpecific((T)t);
        } else {
            size++;
            generatedCodeAdd(t);
        }
    }

    public abstract void generatedCodeAdd(SchemaTuple<?> t);

    public void addSpecific(T t) {
        size++;
        generatedCodeAddSpecific(t);
    }

    public abstract void generatedCodeAddSpecific(T t);

    @Override
    public void addAll(DataBag b) {
        for (Tuple t : b) {
            add(t);
        }
    }

    public void addAll(SchemaDataBag<?> b) {
        setColumns(b.getColumns());
        size = getColumns()[0].size();
    }

    public abstract SpillableColumn[] getColumns();

    public abstract void setColumns(SpillableColumn[] columns);

    @Override
    public void clear() {
        for (SpillableColumn sc : getColumns()) {
            sc.clear();
        }
    }

    @Override
    public void markStale(boolean stale) {}
}
