package org.apache.pig.algebraic;

public class AlgebraicFuncSpec<T> {
    private Class<? extends T> clazz;

    public AlgebraicFuncSpec(Class<? extends T> clazz) {
        this.clazz = clazz;
    }
    //TODO make a builder?

    //TODO implement
    /**
     * Returns a String suitable for instantiation via the algebraic interface.
     */
    public String getFuncString() {
        return null;
    }
}
