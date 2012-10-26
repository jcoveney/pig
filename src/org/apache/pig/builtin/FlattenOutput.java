package org.apache.pig.builtin;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface FlattenOutput {
    boolean prefix() default true;

    public enum FlattenStates {
        DO_NOTHING,
        FLATTEN_WITH_PREFIX,
        FLATTEN_WITHOUT_PREFIX;

        public boolean shouldFlatten() {
            return this != FlattenStates.DO_NOTHING;
        }

        public boolean prefix() {
            return this == FLATTEN_WITH_PREFIX;
        }
    }
}
