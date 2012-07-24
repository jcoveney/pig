package org.apache.pig.data;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.pig.data.IntSpillableColumn.IntIterator;
import org.junit.Test;

public class TestSchemaBag {
    @Test
    public void testIntSpillableColumnAddingWithSpillThread() {
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
            intSpillable.add(i);
            total += i;
            added.incrementAndGet();
        }
        stop.set(true);
        for (int i = 1000000; i < 5000000; i++) {
            intSpillable.add(i);
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
            total -= iterator.next();
            added.incrementAndGet();
        }

        assertEquals(5000000L, totalSpilled.get());
        assertEquals(0, total);
    }

    @Test
    public void testIntSpillableColumnNoThreads() {
        IntSpillableColumn intSpillable = new IntSpillableColumn(); //do not want to register with spill manager so we control spilling
        long total = 0;
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 1000000; j++) {
                intSpillable.add(j);
                total += j;
            }
            assertEquals(1000000, intSpillable.spill());
        }
        for (int j = 1000000; j < 5000000; j++) {
            intSpillable.add(j);
            total += j;
        }
        IntIterator iterator = intSpillable.iterator();
        while (iterator.hasNext()) {
            total -= iterator.next();
        }
        assertEquals(0, total);
    }
}
