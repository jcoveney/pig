package org.apache.pig.data;

import static org.junit.Assert.assertEquals;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Iterator;

import org.junit.Test;

public class TestNewDefaultDataBag {
    private static final TupleFactory mTupleFactory = TupleFactory.getInstance();
    private static final BinInterSedes bis = new BinInterSedes();

    @Test
    public void testAddToBag() throws Exception {
        NewDefaultDataBag bag = new NewDefaultDataBag();

        long total = 0;

        for (int i = 0; i < 10000; i++) {
            Tuple t = mTupleFactory.newTuple(5);
            for (int j = 0; j < 5; j++) {
                t.set(j, i*j);
                total += i*j;
            }
            bag.add(t);
        }

        for (Tuple t : bag) {
            for (Object o : t) {
                total -= ((Number)o).longValue();
            }
        }

        assertEquals(0, total);
    }

    @Test
    public void testAddToBagThenSpill() throws Exception {
        NewDefaultDataBag bag = new NewDefaultDataBag();

        long total = 0;

        for (int i = 0; i < 10000; i++) {
            Tuple t = mTupleFactory.newTuple(5);
            for (int j = 0; j < 5; j++) {
                t.set(j, i*j);
                total += i*j;
            }
            bag.add(t);
        }

        bag.spill();

        for (Tuple t : bag) {
            for (Object o : t) {
                total -= ((Number)o).longValue();
            }
        }

        assertEquals(0, total);
    }

    @Test
    public void testAddToBagThenSpill2() throws Exception {
        NewDefaultDataBag bag = new NewDefaultDataBag();

        long total = 0;

        for (int i = 0; i < 10000; i++) {
            Tuple t = mTupleFactory.newTuple(5);
            for (int j = 0; j < 5; j++) {
                t.set(j, i*j);
                total += i*j;
            }
            bag.add(t);
        }

        Iterator<Tuple> it = bag.iterator();

        bag.spill();

        while (it.hasNext()) {
            for (Object o : it.next()) {
                total -= ((Number)o).longValue();
            }
        }

        assertEquals(0, total);
    }

    @Test
    public void testAddToBagThenSpill3() throws Exception {
        NewDefaultDataBag bag = new NewDefaultDataBag();

        long total = 0;

        for (int i = 0; i < 10000; i++) {
            Tuple t = mTupleFactory.newTuple(5);
            for (int j = 0; j < 5; j++) {
                t.set(j, i*j);
                total += i*j;
            }
            if (i % 666 == 0) {
                bag.spill();
            }
            bag.add(t);
        }

        Iterator<Tuple> it = bag.iterator();

        bag.spill();

        while (it.hasNext()) {
            for (Object o : it.next()) {
                total -= ((Number)o).longValue();
            }
        }

        assertEquals(0, total);
    }

    @Test
    public void testAddToBagThenSpill4() throws Exception {
        NewDefaultDataBag bag = new NewDefaultDataBag();

        long total = 0;

        for (int i = 0; i < 10000; i++) {
            Tuple t = mTupleFactory.newTuple(5);
            for (int j = 0; j < 5; j++) {
                t.set(j, i*j);
                total += i*j;
            }
            if (i % 666 == 0) {
                bag.spill();
            }
            bag.add(t);
        }

        for (Tuple t : bag) {
            for (Object o : t) {
                total -= ((Number)o).longValue();
            }
        }

        assertEquals(0, total);
    }

    @Test
    public void testSerDe() throws Exception {
        NewDefaultDataBag bag = new NewDefaultDataBag();

        long total = 0;
        for (int i = 0; i < 10000; i++) {
            Tuple t = mTupleFactory.newTuple(5);
            for (int j = 0; j < 5; j++) {
                t.set(j, i*j);
                total += i*j;
            }
            bag.add(t);
        }

        File f = File.createTempFile("tmp","tmp");
        DataOutputStream out = new DataOutputStream(new FileOutputStream(f));

        bag.write(out);
        out.close();

        DataInputStream in = new DataInputStream(new FileInputStream(f));

        bag = new NewDefaultDataBag();
        bag.readFields(in);

        for (Tuple t : bag) {
            for (Object o : t) {
                total -= ((Number)o).longValue();
            }
        }

        assertEquals(0, total);
    }

    @Test
    public void testSerDe2() throws Exception {
        NewDefaultDataBag bag = new NewDefaultDataBag();

        long total = 0;
        for (int i = 0; i < 10000; i++) {
            Tuple t = mTupleFactory.newTuple(5);
            for (int j = 0; j < 5; j++) {
                t.set(j, i*j);
                total += i*j;
            }
            bag.add(t);
        }

        File f = File.createTempFile("tmp","tmp");
        DataOutputStream out = new DataOutputStream(new FileOutputStream(f));

        bis.writeDatum(out, bag, DataType.BAG);
        out.close();

        DataInputStream in = new DataInputStream(new FileInputStream(f));

        bag = (NewDefaultDataBag) bis.readDatum(in);

        for (Tuple t : bag) {
            for (Object o : t) {
                total -= ((Number)o).longValue();
            }
        }

        assertEquals(0, total);
    }
}