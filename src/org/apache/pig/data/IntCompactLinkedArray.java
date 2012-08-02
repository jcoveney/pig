package org.apache.pig.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntCompactLinkedArray {
    public IntLink first;
    private IntLink last;
    private int ct = 0;
    private int stacks = 1;

    public IntCompactLinkedArray(int perLink) {
        first = new IntLink(perLink);
        last = first;
    }

    public long getMemorySize() {
        //first pointer (8) + last pointer (8) + ct (4) + stacks (4) + plus the number of stacks
        return 24 + stacks * (first.buf.length * 4 + 8); //TODO do we need to round for first.buf.length to a multiple of 8?
    }

    public void reset() {
        first = new IntLink(first.buf.length);
        last = first;
        ct = 0;
    }

    public void add(int val) {
        int[] buf = last.buf;
        buf[ct++] = val;
        int length = buf.length;
        if (ct == length) {
            IntLink temp = new IntLink(length);
            last.next = temp;
            last = temp;
            stacks++;
            ct = 0;
        }
    }

    public int spill(DataOutput out) throws IOException {
        IntLink iter = first;
        while (iter.next != null) {
            int[] buf = iter.buf;
            int length = buf.length;
            out.writeInt(length);
            for (int i = 0; i < length; i++) {
                out.writeInt(buf[i]);
            }
            iter = iter.next;
            first = iter; //want to orphan the values as we go along so GC could happen if necessary
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
        out.writeInt(ct);
        int[] buf = last.buf;
        for (int i = 0; i < ct; i++) {
            out.writeInt(buf[i]);
        }

        out.writeInt(-1);

        reset();

        return 1;
    }

    public void writeAll(DataOutput out) throws IOException {
        IntLink current = first;

        while (current != null) {
            int[] buf = current.buf;
            int sz = current.next == null ? ct : buf.length;
            out.writeInt(sz);
            for (int i = 0; i < sz; i++) {
                out.writeInt(buf[i]);
            }
            current = current.next;
        }

        out.writeInt(-1);

    }

    public void readAll(DataInput in) throws IOException {
        reset();
        int size = last.buf.length;
        int length;
        boolean first = true;
        while ((length = in.readInt()) != -1) {
            if (first) {
                first = false;
            } else {
                IntLink temp = new IntLink(size);
                last.next = temp;
                last = temp;
            }
            int[] buf = last.buf;
            for (int i = 0; i < length; i++) {
                buf[i] = in.readInt();
            }
            ct = length;
        }
    }

    public static class IntLink {
        public int[] buf;
        public IntLink next;

        public IntLink(int perLink) {
            buf = new int[perLink];
        }
    }
}