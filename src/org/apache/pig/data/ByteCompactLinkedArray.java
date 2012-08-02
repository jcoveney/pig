package org.apache.pig.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.pig.data.utils.BytesHelper;

public class ByteCompactLinkedArray {
    public ByteLink first;
    private ByteLink last;
    private int ct = 0;
    private byte cachedValue = (byte)0xFF;
    private int stacks = 1;

    public ByteCompactLinkedArray(int perLink) {
        first = new ByteLink(perLink);
        last = first;
    }

    public long getMemorySize() {
        //first pointer (8) + last pointer (8) + ct (4) + stacks (4) + cachedValue (1), rounded to 8, plus the number of stacks
        return 32 + stacks * (first.buf.length + 8); //TODO do we need to round for first.buf.length to a multiple of 8?
    }

    public void reset() {
        first = new ByteLink(first.buf.length);
        last = first;
        ct = 0;
    }

    public void add(boolean val) {
        int mod = ct++ & 7;
        cachedValue = BytesHelper.setBitByPos(cachedValue, val, mod);

        if (mod == 7) {
            byte[] buf = last.buf;
            int ctShift = (ct >> 3) - 1;
            buf[ctShift] = cachedValue;
            cachedValue = (byte)0xFF;
            int length = buf.length;
            if (++ctShift == length) {
                ByteLink temp = new ByteLink(length);
                last.next = temp;
                last = temp;
                stacks++;
                ct = 0;
            }
        }
    }

    public int spill(DataOutput out) throws IOException {
        ByteLink iter = first;
        while (iter.next != null) {
            byte[] buf = iter.buf;
            out.writeInt(buf.length);
            out.write(buf);
            iter = iter.next;
        }
        first = last;
        int stackCt = stacks - 1;
        stacks = 1;
        return stackCt;
    }

    public int finalSpill(DataOutput out) throws IOException {
        if (ct == 0) {
            return 0;
        }
        int ctShift = ct >> 3;
        int ourCt = ctShift + 1;
        int mod = ct & 7;
        boolean rollOver = ourCt == last.buf.length;
        boolean serializeCached = mod > 0 && !rollOver;

        if (serializeCached) {
            out.writeInt(ourCt);
        } else {
            out.writeInt(ctShift);
        }
        out.write(last.buf, 0, ctShift);

        if (serializeCached) {
            out.writeByte(cachedValue);
        } else if (rollOver) {
            out.writeInt(1);
            out.writeByte(cachedValue);
        }

        out.writeInt(-1);
        out.writeInt(ct);

        reset();

        return 1;
    }

    public void writeAll(DataOutput out) throws IOException {
        byte[] buf = last.buf;
        int mod = ct & 7;
        int ctShift = ct >> 3;
        int ourShift = ctShift + 1;
        boolean rollOver = ourShift == buf.length;
        boolean serializeCached = mod > 0 && !rollOver;

        ByteLink current = first;
        while (current.next != null) {
            byte[] buf2 = current.buf;
            out.writeInt(buf2.length);
            out.write(buf2);
            current = current.next;
        }

        if (serializeCached) {
            out.writeInt(ourShift);
        } else {
            out.writeInt(ctShift);
        }
        out.write(buf, 0, ctShift);
        if (serializeCached) {
            out.writeByte(cachedValue);
        } else if (rollOver) {
            out.writeInt(1);
            out.writeByte(cachedValue);
        }

        out.writeInt(-1);
        out.writeInt(ct); //we need this to know what the actual count was
    }

    public void readAll(DataInput in) throws IOException {
        reset();
        int size = last.buf.length;
        int length;
        boolean first = true;
        while ((length = in.readInt()) != -1) {
            stacks++;
            if (first) {
                first = false;
            } else {
                ByteLink temp = new ByteLink(size);
                last.next = temp;
                last = temp;
            }
            byte[] buf = new byte[size];
            in.readFully(buf, 0, length);
            last.buf = buf;
        }
        if (!first) {
            ct = in.readInt();
        }

        //as long as nobody adds to this, we shouldn't need to reconstruct the full state
        //otherwise, we can reconstruct that state if necessary
    }

    public static class ByteLink {
        public byte[] buf;
        public ByteLink next;

        public ByteLink(int perLink) {
            buf = new byte[perLink];
        }
    }
}