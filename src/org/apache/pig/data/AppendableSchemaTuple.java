package org.apache.pig.data;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.data.utils.SedesHelper;
import org.apache.pig.data.utils.HierarchyHelper.MustOverride;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class AppendableSchemaTuple<T extends AppendableSchemaTuple<T>> extends SchemaTuple<T> {
    private static final long serialVersionUID = 1L;

    private Tuple append;

    private static final TupleFactory mTupleFactory = TupleFactory.getInstance();

    @Override
    public void append(Object val) {
        if (append == null) {
            append = mTupleFactory.newTuple();
        }

        append.append(val);
    }

    protected int appendSize() {
        return append == null ? 0 : append.size();
    }

    protected boolean appendIsNull() {
        return appendSize() == 0;
    }

    protected Object getAppend(int i) throws ExecException {
        return appendIsNull(i) ? null : append.get(i);
    }

    private boolean appendIsNull(int i) throws ExecException {
        return appendIsNull() || append.isNull(i);
    }

    //protected Tuple getAppend() {
    public Tuple getAppend() {
        return append;
    }

    protected void setAppend(Tuple t) {
        append = t;
    }

    private void appendReset() {
        append = null;
    }

    private void setAppend(int fieldNum, Object val) throws ExecException {
        append.set(fieldNum, val);
    }

    /**
     * This adds the additional overhead of the append Tuple
     */
    @Override
    @MustOverride
    public long getMemorySize() {
        return SizeUtil.roundToEight(append.getMemorySize()) + super.getMemorySize();
    }

    private byte appendType(int i) throws ExecException {
        return append == null ? DataType.UNKNOWN : append.getType(i);
    }

    @MustOverride
    protected SchemaTuple<T> set(SchemaTuple<?> t, boolean checkType) throws ExecException {
        appendReset();
        for (int j = sizeNoAppend(); j < t.size(); j++) {
            append(t.get(j));
        }
        return super.set(t, checkType);
    }

    @MustOverride
    protected SchemaTuple<T> setSpecific(T t) {
        appendReset();
        setAppend(t.getAppend());
        return super.setSpecific(t);
    }

    public SchemaTuple<T> set(List<Object> l) throws ExecException {
        if (l.size() < sizeNoAppend())
            throw new ExecException("Given list of objects has too few fields ("+l.size()+" vs "+sizeNoAppend()+")");

        for (int i = 0; i < sizeNoAppend(); i++)
            set(i, l.get(i));

        appendReset();

        for (int i = sizeNoAppend(); i < l.size(); i++) {
            append(l.get(i++));
        }

        return this;
    }

    @MustOverride
    protected int compareTo(SchemaTuple<?> t, boolean checkType) {
        if (appendSize() > 0) {
            int i;
            int m = sizeNoAppend();
            for (int k = 0; k < size() - sizeNoAppend(); k++) {
                try {
                    i = DataType.compare(getAppend(k), t.get(m++));
                } catch (ExecException e) {
                    throw new RuntimeException("Unable to get append value", e);
                }
                if (i != 0) {
                    return i;
                }
            }
        }
        return 0;
    }

    @MustOverride
    protected int compareToSpecific(T t) {
        int i;
        for (int z = 0; z < appendSize(); z++) {
            try {
                i = DataType.compare(getAppend(z), t.getAppend(z));
            } catch (ExecException e) {
                throw new RuntimeException("Unable to get append", e);
            }
            if (i != 0) {
                return i;
            }
        }
        return 0;
    }

    @MustOverride
    public int hashCode() {
        return append.hashCode();
    }

    @MustOverride
    public void set(int fieldNum, Object val) throws ExecException {
        int diff = fieldNum - sizeNoAppend();
        if (diff < appendSize()) {
            setAppend(diff, val);
            return;
        }
        throw new ExecException("Invalid index " + fieldNum + " given");
    }

    @MustOverride
    public Object get(int fieldNum) throws ExecException {
        int diff = fieldNum - sizeNoAppend();
        if (diff < appendSize())
            return getAppend(diff);
        throw new ExecException("Invalid index " + fieldNum + " given");
    }

    @MustOverride
    public boolean isNull(int fieldNum) throws ExecException {
        int diff = fieldNum - sizeNoAppend();
        if (diff < appendSize()) {
            return appendIsNull(diff);
        }
        throw new ExecException("Invalid index " + fieldNum + " given");
    }

    //TODO: do we even need this?
    @MustOverride
    public void setNull(int fieldNum) throws ExecException {
        int diff = fieldNum - sizeNoAppend();
        if (diff < appendSize()) {
            setAppend(diff, null);
        } else {
            throw new ExecException("Invalid index " + fieldNum + " given");
        }
    }

    @MustOverride
    public byte getType(int fieldNum) throws ExecException {
        int diff = fieldNum - sizeNoAppend();
        if (diff < appendSize()) {
            return appendType(diff);
        }
        throw new ExecException("Invalid index " + fieldNum + " given");
    }

    protected void setPrimitiveBase(int fieldNum, Object val, String type) throws ExecException {
        int diff = fieldNum - sizeNoAppend();
        if (diff < appendSize()) {
            setAppend(diff, val);
        }
        throw new ExecException("Given field " + fieldNum + " not a " + type + " field!");
    }

    protected Object getPrimitiveBase(int fieldNum, String type) throws ExecException {
        int diff = fieldNum - sizeNoAppend();
        if (diff < appendSize()) {
            return getAppend(diff);
        }
        throw new ExecException("Given field " + fieldNum + " not a " + type + " field!");
    }

    @MustOverride
    protected void writeElements(DataOutput out) throws IOException {
        if (!appendIsNull()) {
            SedesHelper.writeGenericTuple(out, getAppend());
        }
    }

    @MustOverride
    protected int compareSizeSpecific(T t) {
        int mySz = appendSize();
        int tSz = t.appendSize();
        if (mySz != tSz) {
            return mySz > tSz ? 1 : -1;
        }
        return 0;
    }
}