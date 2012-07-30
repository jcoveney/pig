package org.apache.pig.data;

import static org.junit.Assert.assertEquals;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.pig.data.IntSpillableColumn.IntContainer;
import org.apache.pig.data.IntSpillableColumn.IntIterator;
import org.junit.Test;

public class TestSpillableColumns {
    //This is an exact copy of testIntSpillableColumnAddingWithSpillThreadWithNulls without threads
    @Test
    public void testIntSpillableColumnAddingWithSpillNoThreadWithNulls() {
        int added = 0;

        final IntSpillableColumn intSpillable = new IntSpillableColumn(); //do not want to register with spill manager so we control spilling

        long total = 0;
        for (int i = 0; i < 10000; i++) {
            boolean isNull = i % 3 == 0;
            intSpillable.add(i, isNull);
            if (!isNull) {
                total += i;
            }
            if (added++ % 1501 == 0) {
                intSpillable.spill();
            }
        }

        for (int i = 10000; i < 50000; i++) {
            boolean isNull = i % 3 == 0;
            intSpillable.add(i, isNull);
            if (!isNull) {
                total += i;
            }
        }

        added = 0;
        IntIterator iterator = (IntIterator)intSpillable.iterator();
        while (iterator.hasNext()) {
            IntContainer container = iterator.next();
            if (!container.isNull) {
                total -= container.value;
            }
            if (added++ == 20005) {
                intSpillable.spill();
            }
        }

        assertEquals(0L, total);
    }

    @Test
    public void testIntSpillableColumnAddingWithSpillNoThreadWithNullsNoFinalSpillTinyIncrements() {
        int added = 0;
        final IntSpillableColumn intSpillable = new IntSpillableColumn(); //do not want to register with spill manager so we control spilling

        long total = 0;
        for (int i = 0; i < 1000; i++) {
            boolean isNull = i % 3 == 0;
            intSpillable.add(i, isNull);
            if (!isNull) {
                total += i;
            }
            if (added++ % 7 == 0) {
                intSpillable.spill();
            }
        }

        for (int i = 1000; i < 2000; i++) {
            boolean isNull = i % 3 == 0;
            intSpillable.add(i, isNull);
            if (!isNull) {
                total += i;
            }
            if (added++ % 99 == 0) {
                intSpillable.spill();
            }
        }

        for (int i = 2000; i < 3000; i++) {
            boolean isNull = i % 3 == 0;
            intSpillable.add(i, isNull);
            if (!isNull) {
                total += i;
            }
        }

        IntIterator iterator = (IntIterator)intSpillable.iterator();
        while (iterator.hasNext()) {
            IntContainer container = iterator.next();
            if (!container.isNull) {
                total -= container.value;
            }
        }

        assertEquals(0L, total);
    }

    @Test
    public void testIntSpillableColumnAddingWithSpillNoThreadWithNullsNoFinalSpill() {
        int added = 0;
        final IntSpillableColumn intSpillable = new IntSpillableColumn(); //do not want to register with spill manager so we control spilling

        long total = 0;
        for (int i = 0; i < 10000; i++) {
            boolean isNull = i % 3 == 0;
            intSpillable.add(i, isNull);
            if (!isNull) {
                total += i;
            }
            if (added++ % 1501 == 0) {
                intSpillable.spill();
            }
        }

        for (int i = 10000; i < 50000; i++) {
            boolean isNull = i % 3 == 0;
            intSpillable.add(i, isNull);
            if (!isNull) {
                total += i;
            }
        }

        IntIterator iterator = (IntIterator)intSpillable.iterator();
        while (iterator.hasNext()) {
            IntContainer container = iterator.next();
            if (!container.isNull) {
                total -= container.value;
            }
        }

        assertEquals(0L, total);
    }

    @Test
    public void testIntSpillableColumnTiny() {
        final IntSpillableColumn intSpillable = new IntSpillableColumn(); //do not want to register with spill manager so we control spilling
        long total = 0L;

        for (int i = 0; i < 20; i++) {
            intSpillable.add(i, false);
            total += i;
        }

        intSpillable.spill();

        for (int i = 20; i < 40; i++) {
            intSpillable.add(i, false);
            total += i;
        }

        IntIterator iterator = (IntIterator)intSpillable.iterator();
        while (iterator.hasNext()) {
            total -= iterator.next().value;
        }

        assertEquals(0L, total);
    }

    @Test
    public void testIntSpillableColumnTinyWithFinalSpill() {
        final IntSpillableColumn intSpillable = new IntSpillableColumn(); //do not want to register with spill manager so we control spilling
        long total = 0L;

        for (int i = 0; i < 20; i++) {
            intSpillable.add(i, false);
            total += i;
        }

        intSpillable.spill();

        for (int i = 20; i < 40; i++) {
            intSpillable.add(i, false);
            total += i;
        }

        int added = 0;
        IntIterator iterator = (IntIterator)intSpillable.iterator();
        while (iterator.hasNext()) {
            if (added++ == 20) {
                intSpillable.spill();
            }
            total -= iterator.next().value;
        }

        assertEquals(0L, total);
    }

    //This is an exact copy of testIntSpillableColumnAddingWithSpillThreadNoNulls without threads
    @Test
    public void testIntSpillableColumnAddingWithSpillNoThreadNoNulls() {
        int added = 0;

        final IntSpillableColumn intSpillable = new IntSpillableColumn(); //do not want to register with spill manager so we control spilling

        long total = 0;
        for (int i = 0; i < 10000; i++) {
            intSpillable.add(i, false);
            total += i;
            if (added++ % 1501 == 0) {
                intSpillable.spill();
            }
        }

        for (int i = 10000; i < 50000; i++) {
            intSpillable.add(i, false);
            total += i;
        }

        added = 0;
        IntIterator iterator = (IntIterator)intSpillable.iterator();
        while (iterator.hasNext()) {
            total -= iterator.next().value;
            if (added++ == 20005) {
                intSpillable.spill();
            }
        }

        assertEquals(0L, total);
    }

    @Test
    public void testIntSpillableColumnAddingWithSpillThreadWithNulls() {
        final AtomicInteger added = new AtomicInteger(0);
        final AtomicBoolean stop = new AtomicBoolean(false);

        final IntSpillableColumn intSpillable = new IntSpillableColumn(); //do not want to register with spill manager so we control spilling
        Thread spillThread = new Thread(new Runnable() {
            @Override
            public void run() {
                long spilled = 0L;
                while (!stop.get()) {
                    if ((added.get() - spilled) > 1500L) {
                        spilled += intSpillable.spill();
                    }
                }
            }
        });
        spillThread.start();

        long total = 0;
        for (int i = 0; i < 10000; i++) {
            boolean isNull = i % 3 == 0;
            intSpillable.add(i, isNull);
            if (!isNull) {
                total += i;
            }
            added.incrementAndGet();
        }
        stop.set(true);
        for (int i = 10000; i < 50000; i++) {
            boolean isNull = i % 3 == 0;
            intSpillable.add(i, isNull);
            if (!isNull) {
                total += i;
            }
        }

        added.set(0);
        spillThread = new Thread(new Runnable() {
           @Override
           public void run() {
               while (added.get() < 20000) {
               }
               intSpillable.spill();
           }
        });
        spillThread.start();

        IntIterator iterator = (IntIterator)intSpillable.iterator();
        while (iterator.hasNext()) {
            IntContainer container = iterator.next();
            if (!container.isNull) {
                total -= container.value;
            }
            added.incrementAndGet();
        }

        assertEquals(0L, total);
    }

    @Test
    public void testIntSpillableColumnAddingWithSpillThreadNoNulls() {
        final AtomicInteger added = new AtomicInteger(0);
        final AtomicBoolean stop = new AtomicBoolean(false);

        final IntSpillableColumn intSpillable = new IntSpillableColumn(); //do not want to register with spill manager so we control spilling
        Thread spillThread = new Thread(new Runnable() {
            @Override
            public void run() {
                long spilled = 0L;
                while (!stop.get()) {
                    if ((added.get() - spilled) > 1501L) {
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                        }
                        spilled += intSpillable.spill();
                    }
                }
            }
        });
        spillThread.start();

        long total = 0;
        for (int i = 0; i < 10000; i++) {
            intSpillable.add(i, false);
            total += i;
            added.incrementAndGet();
        }
        stop.set(true);
        for (int i = 10000; i < 50000; i++) {
            intSpillable.add(i, false);
            total += i;
        }

        added.set(0);
        spillThread = new Thread(new Runnable() {
           @Override
           public void run() {
               while (added.get() < 20005) {
               }
               intSpillable.spill();
           }
        });
        spillThread.start();

        IntIterator iterator = (IntIterator)intSpillable.iterator();
        while (iterator.hasNext()) {
            total -= iterator.next().value;
            added.incrementAndGet();
        }

        assertEquals(0L, total);
    }

    @Test
    public void testIntSpillableColumnNoThreadsWithNulls() {
        IntSpillableColumn intSpillable = new IntSpillableColumn(); //do not want to register with spill manager so we control spilling
        long total = 0;
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10000; j++) {
                boolean isNull = j % 123 == 0;
                intSpillable.add(j, isNull);
                if (!isNull) {
                    total += j;
                }
            }
            intSpillable.spill();
        }
        for (int j = 10000; j < 50000; j++) {
            boolean isNull = j % 123 == 0;
            intSpillable.add(j, isNull);
            if (!isNull) {
                total += j;
            }
        }
        IntIterator iterator = (IntIterator)intSpillable.iterator();
        while (iterator.hasNext()) {
            IntContainer cont = iterator.next();
            if (!cont.isNull) {
                total -= cont.value;
            }
        }
        assertEquals(0L, total);
    }

    @Test
    public void testIntSpillableColumnNoThreadsNoNulls() {
        IntSpillableColumn intSpillable = new IntSpillableColumn(); //do not want to register with spill manager so we control spilling
        long total = 0;
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10001; j++) {
                intSpillable.add(j, false);
                total += j;
            }
            intSpillable.spill();
        }
        for (int j = 10000; j < 50000; j++) {
            intSpillable.add(j, false);
            total += j;
        }
        IntIterator iterator = (IntIterator)intSpillable.iterator();
        while (iterator.hasNext()) {
            total -= iterator.next().value;
        }
        assertEquals(0, total);
    }

    @Test
    public void testIntSpillableColumnSerDeNoNulls() throws Exception {
        IntSpillableColumn intSpillable = new IntSpillableColumn();
        long total = 0;
        int records = 1000;
        for (int i = 0; i < records; i++) {
            intSpillable.add(i, false);
            total += i;
        }

        File f = File.createTempFile("tmp", "tmp");
        DataOutputStream out = new DataOutputStream(new FileOutputStream(f));

        intSpillable.writeData(out);
        out.close();

        DataInputStream in = new DataInputStream(new FileInputStream(f));
        IntSpillableColumn intSpillable2 = new IntSpillableColumn();
        intSpillable2.readData(in, records);

        assertEquals(intSpillable.size(), intSpillable2.size());

        IntIterator it = (IntIterator)intSpillable2.iterator();

        while (it.hasNext()) {
            total -= it.next().value;
        }

        assertEquals(0L, total);
    }

    @Test
    public void testIntSpillableColumnSerDeWithNulls() throws Exception {
        IntSpillableColumn intSpillable = new IntSpillableColumn();
        long total = 0;
        int records = 1000;
        for (int i = 0; i < records; i++) {
            boolean isNull = i % 3 == 0;
            intSpillable.add(i, isNull);
            if (!isNull) {
                total += i;
            }
        }

        File f = File.createTempFile("tmp", "tmp");
        DataOutputStream out = new DataOutputStream(new FileOutputStream(f));

        intSpillable.writeData(out);
        out.close();

        DataInputStream in = new DataInputStream(new FileInputStream(f));
        IntSpillableColumn intSpillable2 = new IntSpillableColumn();
        intSpillable2.readData(in, records);

        assertEquals(intSpillable.size(), intSpillable2.size());

        IntIterator it = (IntIterator)intSpillable2.iterator();

        while (it.hasNext()) {
            IntContainer cont = it.next();
            if (!cont.isNull) {
                total -= cont.value;
            }
        }

        assertEquals(0L, total);
    }
}
