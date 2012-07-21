package org.apache.pig.data.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TypeAwareTuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.carrotsearch.hppc.DoubleObjectOpenHashMap;
import com.carrotsearch.hppc.FloatObjectOpenHashMap;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.LongObjectOpenHashMap;

public final class CollectionsHelper {

    private CollectionsHelper() {}

    public static interface BasicList<T> {
        public boolean add(T t);
        public T get(int i);
        public int size();
        public List<T> getList();
    }

    public static interface BasicMap<K,V> {
        public V put(K key, V val);
        public V get(K key);
    }

    public static class DefaultBasicMap implements BasicMap<Tuple,BasicList<Tuple>> {
        Map<Tuple, BasicList<Tuple>> internal = new HashMap<Tuple, BasicList<Tuple>>();

        @Override
        public BasicList<Tuple> put(Tuple key, BasicList<Tuple> val) {
            return internal.put(key, val);
        }

        @Override
        public BasicList<Tuple> get(Tuple key) {
            return internal.get(key);
        }
    }

    public static class IntBasicMap implements BasicMap<Tuple,BasicList<Tuple>> {
        IntObjectOpenHashMap<BasicList<Tuple>> internal = new IntObjectOpenHashMap<BasicList<Tuple>>();

        public int getPrimitive(Tuple t) {
            try {
                return ((TypeAwareTuple)t).getInt(0);
            } catch (Exception e) {
                throw new RuntimeException("Given present Schema, expected a Tuple containing"
                        + " a single primitive entry", e);
            }
        }

        @Override
        public BasicList<Tuple> put(Tuple key, BasicList<Tuple> val) {
            return internal.put(getPrimitive(key), val);
        }

        @Override
        public BasicList<Tuple> get(Tuple key) {
            return internal.get(getPrimitive(key));
        }
    }

    public static class LongBasicMap implements BasicMap<Tuple,BasicList<Tuple>> {
        LongObjectOpenHashMap<BasicList<Tuple>> internal = new LongObjectOpenHashMap<BasicList<Tuple>>();

        public long getPrimitive(Tuple t) {
            try {
                return ((TypeAwareTuple)t).getLong(0);
            } catch (Exception e) {
                throw new RuntimeException("Given present Schema, expected a Tuple containing"
                        + " a single primitive entry", e);
            }
        }

        @Override
        public BasicList<Tuple> put(Tuple key, BasicList<Tuple> val) {
            return internal.put(getPrimitive(key), val);
        }

        @Override
        public BasicList<Tuple> get(Tuple key) {
            return internal.get(getPrimitive(key));
        }
    }

    public static class FloatBasicMap implements BasicMap<Tuple,BasicList<Tuple>> {
        FloatObjectOpenHashMap<BasicList<Tuple>> internal = new FloatObjectOpenHashMap<BasicList<Tuple>>();

        public float getPrimitive(Tuple t) {
            try {
                return ((TypeAwareTuple)t).getFloat(0);
            } catch (Exception e) {
                throw new RuntimeException("Given present Schema, expected a Tuple containing"
                        + " a single primitive entry", e);
            }
        }

        @Override
        public BasicList<Tuple> put(Tuple key, BasicList<Tuple> val) {
            return internal.put(getPrimitive(key), val);
        }

        @Override
        public BasicList<Tuple> get(Tuple key) {
            return internal.get(getPrimitive(key));
        }
    }

    public static class DoubleBasicMap implements BasicMap<Tuple,BasicList<Tuple>> {
        DoubleObjectOpenHashMap<BasicList<Tuple>> internal = new DoubleObjectOpenHashMap<BasicList<Tuple>>();

        public double getPrimitive(Tuple t) {
            try {
                return ((TypeAwareTuple)t).getDouble(0);
            } catch (Exception e) {
                throw new RuntimeException("Given present Schema, expected a Tuple containing"
                        + " a single primitive entry", e);
            }
        }

        @Override
        public BasicList<Tuple> put(Tuple key, BasicList<Tuple> val) {
            return internal.put(getPrimitive(key), val);
        }

        @Override
        public BasicList<Tuple>get(Tuple key) {
            return internal.get(getPrimitive(key));
        }
    }

    public static BasicMap<Tuple,BasicList<Tuple>> getBasicMapForSchema(Schema s) {
        try {
            switch (s.getField(0).type) {
            case DataType.INTEGER:
                return new IntBasicMap();
            case DataType.LONG:
                return new LongBasicMap();
            case DataType.FLOAT:
                return new FloatBasicMap();
            case DataType.DOUBLE:
                return new DoubleBasicMap();
            default:
                return new DefaultBasicMap();
            }
        } catch (FrontendException e) {
            return new DefaultBasicMap();
        }
    }
}