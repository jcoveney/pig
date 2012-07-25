package org.apache.pig.data;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.pig.data.IntSpillableColumn.IntContainer;
import org.apache.pig.data.IntSpillableColumn.IntIterator;
import org.junit.Test;

public class TestSchemaBag {
    //This is an exact copy of testIntSpillableColumnAddingWithSpillThreadWithNulls without threads
    @Test
    public void testIntSpillableColumnAddingWithSpillNoThreadWithNulls() {
        int added = 0;
        long totalSpilled = 0L;

        final IntSpillableColumn intSpillable = new IntSpillableColumn(); //do not want to register with spill manager so we control spilling

        long total = 0;
        for (int i = 0; i < 1000000; i++) {
            boolean isNull = i % 3 == 0;
            intSpillable.add(i, isNull);
            if (!isNull) {
                total += i;
            }
            if (added++ % 150000 == 0) {
                totalSpilled += intSpillable.spill();
            }
        }

        for (int i = 1000000; i < 5000000; i++) {
            boolean isNull = i % 3 == 0;
            intSpillable.add(i, isNull);
            if (!isNull) {
                total += i;
            }
        }

        added = 0;
        IntIterator iterator = intSpillable.iterator();
        while (iterator.hasNext()) {
            IntContainer container = iterator.next();
            if (!container.isNull) {
                total -= container.value;
            }
            if (added++ == 2000000) {
                totalSpilled += intSpillable.spill();
            }
        }

        assertEquals(5000000L, totalSpilled);
        assertEquals(0, total);
    }

    @Test
    public void testIntSpillableColumnAddingWithSpillNoThreadWithNullsNoFinalSpillTinyIncrements() {
        int added = 0;
        final IntSpillableColumn intSpillable = new IntSpillableColumn(); //do not want to register with spill manager so we control spilling

        long total = 0;
        for (int i = 0; i < 1000000; i++) {
            boolean isNull = i % 3 == 0;
            intSpillable.add(i, isNull);
            if (!isNull) {
                total += i;
            }
            if (added++ % 7 == 0) {
                intSpillable.spill();
            }
        }

        for (int i = 1000000; i < 5000000; i++) {
            boolean isNull = i % 3 == 0;
            intSpillable.add(i, isNull);
            if (!isNull) {
                total += i;
            }
        }

        IntIterator iterator = intSpillable.iterator();
        while (iterator.hasNext()) {
            IntContainer container = iterator.next();
            if (!container.isNull) {
                total -= container.value;
            }
        }

        assertEquals(0, total);
    }

    @Test
    public void testIntSpillableColumnAddingWithSpillNoThreadWithNullsNoFinalSpill() {
        int added = 0;
        final IntSpillableColumn intSpillable = new IntSpillableColumn(); //do not want to register with spill manager so we control spilling

        long total = 0;
        for (int i = 0; i < 1000000; i++) {
            boolean isNull = i % 3 == 0;
            intSpillable.add(i, isNull);
            if (!isNull) {
                total += i;
            }
            if (added++ % 150000 == 0) {
                intSpillable.spill();
            }
        }

        for (int i = 1000000; i < 5000000; i++) {
            boolean isNull = i % 3 == 0;
            intSpillable.add(i, isNull);
            if (!isNull) {
                total += i;
            }
        }

        IntIterator iterator = intSpillable.iterator();
        while (iterator.hasNext()) {
            IntContainer container = iterator.next();
            if (!container.isNull) {
                total -= container.value;
            }
        }

        assertEquals(0, total);
    }

    //This is an exact copy of testIntSpillableColumnAddingWithSpillThreadNoNulls without threads
    @Test
    public void testIntSpillableColumnAddingWithSpillNoThreadNoNulls() {
        int added = 0;
        long totalSpilled = 0L;

        final IntSpillableColumn intSpillable = new IntSpillableColumn(); //do not want to register with spill manager so we control spilling

        long total = 0;
        for (int i = 0; i < 1000000; i++) {
            intSpillable.add(i, false);
            total += i;
            if (added++ % 150000 == 0) {
                totalSpilled += intSpillable.spill();
            }
        }

        for (int i = 1000000; i < 5000000; i++) {
            intSpillable.add(i, false);
            total += i;
        }

        added = 0;
        IntIterator iterator = intSpillable.iterator();
        while (iterator.hasNext()) {
            total -= iterator.next().value;
            if (added++ == 2000000) {
                totalSpilled += intSpillable.spill();
            }
        }

        assertEquals(5000000L, totalSpilled);
        assertEquals(0L, total);
    }

    @Test
    public void testIntSpillableColumnAddingWithSpillThreadWithNulls() {
        final AtomicInteger added = new AtomicInteger(0);
        final AtomicBoolean stop = new AtomicBoolean(false);
        final AtomicLong totalSpilled = new AtomicLong(0L);

        final IntSpillableColumn intSpillable = new IntSpillableColumn(); //do not want to register with spill manager so we control spilling
        Thread spillThread = new Thread(new Runnable() {
            @Override
            public void run() {
                long spilled = 0L;
                while (!stop.get()) {
                    if ((added.get() - spilled) > 150000L) {
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                        }
                        spilled += intSpillable.spill();
                    }
                }
                totalSpilled.set(spilled);
            }
        });
        spillThread.start();

        long total = 0;
        for (int i = 0; i < 1000000; i++) {
            boolean isNull = i % 3 == 0;
            intSpillable.add(i, isNull);
            if (!isNull) {
                total += i;
            }
            added.incrementAndGet();
        }
        stop.set(true);
        for (int i = 1000000; i < 5000000; i++) {
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
               while (added.get() < 2000000) {
               }
               totalSpilled.getAndAdd(intSpillable.spill());
           }
        });
        spillThread.start();

        IntIterator iterator = intSpillable.iterator();
        while (iterator.hasNext()) {
            IntContainer container = iterator.next();
            if (!container.isNull) {
                total -= container.value;
            }
            added.incrementAndGet();
        }

        assertEquals(5000000L, totalSpilled.get());
        assertEquals(0, total);
    }

    @Test
    public void testIntSpillableColumnAddingWithSpillThreadNoNulls() {
        final AtomicInteger added = new AtomicInteger(0);
        final AtomicBoolean stop = new AtomicBoolean(false);
        final AtomicLong totalSpilled = new AtomicLong(0L);

        final IntSpillableColumn intSpillable = new IntSpillableColumn(); //do not want to register with spill manager so we control spilling
        Thread spillThread = new Thread(new Runnable() {
            @Override
            public void run() {
                long spilled = 0L;
                while (!stop.get()) {
                    if ((added.get() - spilled) > 150000L) {
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                        }
                        spilled += intSpillable.spill();
                    }
                }
                totalSpilled.set(spilled);
            }
        });
        spillThread.start();

        long total = 0;
        for (int i = 0; i < 1000000; i++) {
            intSpillable.add(i, false);
            total += i;
            added.incrementAndGet();
        }
        stop.set(true);
        for (int i = 1000000; i < 5000000; i++) {
            intSpillable.add(i, false);
            total += i;
        }

        added.set(0);
        spillThread = new Thread(new Runnable() {
           @Override
           public void run() {
               while (added.get() < 2000000) {
               }
               totalSpilled.getAndAdd(intSpillable.spill());
           }
        });
        spillThread.start();

        IntIterator iterator = intSpillable.iterator();
        while (iterator.hasNext()) {
            total -= iterator.next().value;
            added.incrementAndGet();
        }

        assertEquals(5000000L, totalSpilled.get());
        assertEquals(0, total);
    }

    @Test
    public void testIntSpillableColumnNoThreadsWithNulls() {
        IntSpillableColumn intSpillable = new IntSpillableColumn(); //do not want to register with spill manager so we control spilling
        long total = 0;
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 1000000; j++) {
                boolean isNull = j % 123 == 0;
                intSpillable.add(j, isNull);
                if (!isNull) {
                    total += j;
                }
            }
            assertEquals(1000000, intSpillable.spill());
        }
        for (int j = 1000000; j < 5000000; j++) {
            boolean isNull = j % 123 == 0;
            intSpillable.add(j, isNull);
            if (!isNull) {
                total += j;
            }
        }
        IntIterator iterator = intSpillable.iterator();
        while (iterator.hasNext()) {
            IntContainer cont = iterator.next();
            if (!cont.isNull) {
                total -= cont.value;
            }
        }
        assertEquals(0, total);
    }

    @Test
    public void testIntSpillableColumnNoThreadsNoNulls() {
        IntSpillableColumn intSpillable = new IntSpillableColumn(); //do not want to register with spill manager so we control spilling
        long total = 0;
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 1000000; j++) {
                intSpillable.add(j, false);
                total += j;
            }
            assertEquals(1000000, intSpillable.spill());
        }
        for (int j = 1000000; j < 5000000; j++) {
            intSpillable.add(j, false);
            total += j;
        }
        IntIterator iterator = intSpillable.iterator();
        while (iterator.hasNext()) {
            total -= iterator.next().value;
        }
        assertEquals(0, total);
    }

    @Test
    public void testIntSpillableColumnSerDe() {
        IntSpillableColumn intSpillable = new IntSpillableColumn();
        for (int i = 0; i < 1000000; i++) {

        }
    }
}
