package org.apache.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;

public abstract class IteratingAccumulatorEvalFunc<T> extends AccumulatorEvalFunc<T> implements TerminatingAccumulator<T> {
    private boolean isInitialized = false;
    private DelayedQueueIterator dqi;
    private BlockingQueue<Tuple> queue;
    private volatile boolean isFinished = false;
    private volatile boolean noMoreValues = false;
    private T returnValue;
    private Thread executionThread;

    private static final TupleFactory mTupleFactory = TupleFactory.getInstance();

    private static final long WAIT_TO_OFFER = 500L;
    private static final long WAIT_TO_POLL = 500L;

    private void initialize() {
        dqi = new DelayedQueueIterator();
        queue = new LinkedBlockingQueue<Tuple>(10000);

        executionThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    returnValue = exec(dqi);
                } catch (IOException e) {
                    throw new RuntimeException("Error in execution of iterate function", e); //TODO do I need to do something special to surface this?
                }
                isFinished = true;
            }
        });
        executionThread.start();

        isInitialized = true;
    }

    @Override
    public boolean isFinished() {
        return isFinished;
    }

    //need a mechanism to see if the return value has come yet

    @Override
    public void accumulate(Tuple input) throws IOException {
        if (!isInitialized)
            initialize();

        for (Tuple t : (DataBag)input.get(0)) {
            if (isFinished)
                return;

            boolean added = false;
            while (!isFinished && !added)
                try {
                    added = queue.offer(t, WAIT_TO_OFFER, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                }
        }
    }

    @Override
    public T getValue() {
        noMoreValues = true;

        //TODO handle the exception
        try {
            executionThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        return returnValue;
    }

    @Override
    public void cleanup() {
        returnValue = null;
        dqi = null;
        queue = null;
        isFinished = false;
        noMoreValues = false;
        executionThread = null;
        isInitialized = false;
    }

    public abstract T exec(Iterator<Tuple> t) throws IOException;

    private class DelayedQueueIterator implements Iterator<Tuple> {
        private Tuple next;

        @Override
        public boolean hasNext() {
            if (next != null)
                return true;

            while (!noMoreValues) {
                try {
                    next = queue.poll(WAIT_TO_POLL, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (next != null)
                    return true;
            }

            next = queue.poll();

            if (next != null)
                return true;

            return false;
        }

       @Override
       public Tuple next() {
           Tuple t = next;

           if (t == null)
               throw new RuntimeException("Entered inconsistent state!"); //do I need to do something to surface errors from the thread?

           next = null;

           return t;
       }

       @Override
       public void remove() {}
    }
}
