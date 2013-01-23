package org.apache.pig.algebraic;

import java.util.Iterator;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class IntermedIterator<T> implements Iterator<T> {
    private Iterator<Tuple> bagIt;

    protected IntermedIterator(Iterator<Tuple> bagIt) {
        this.bagIt = bagIt;
    }

    @Override
    public boolean hasNext() {
        return bagIt.hasNext();
    }

    @SuppressWarnings("unchecked")
    @Override
    public T next() {
        Tuple t = bagIt.next();
        try {
            return (T)t.get(0);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to get value from Tuple in bag iterator: " + t, e);
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}