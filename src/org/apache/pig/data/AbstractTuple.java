/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.data;

import java.util.Iterator;

import org.apache.pig.impl.util.TupleFormat;
import org.apache.pig.backend.executionengine.ExecException;

import com.google.common.base.Joiner;

public abstract class AbstractTuple implements Tuple {
    @Override
    public Iterator<Object> iterator() {
        return getAll().iterator();
    }

    @Override
    public String toString() {
        return TupleFormat.format(this);
    }

    /**
     * Write a tuple of atomic values into a string. All values in the tuple must be atomic (no bags, tuples, or maps).
     *
     * @param delim
     *            Delimiter to use in the string.
     * @return A string containing the tuple.
     * @throws ExecException
     *             if a non-atomic value is found.
     */
    @Override
    public String toDelimitedString(String delim) throws ExecException {
        return Joiner.on(delim).join(getAll());
    }

    /**
     * Find the type of a given field.
     *
     * @param fieldNum
     *            Number of field to get the type for.
     * @return type, encoded as a byte value. The values are taken from the class DataType. If the field is null, then
     *         DataType.UNKNOWN will be returned.
     * @throws ExecException
     *             if the field number is greater than or equal to the number of fields in the tuple.
     */
    @Override
    public byte getType(int fieldNum) throws ExecException {
        return DataType.findType(get(fieldNum));
    }

    /**
     * Find out if a given field is null.
     *
     * @param fieldNum
     *            Number of field to check for null.
     * @return true if the field is null, false otherwise.
     * @throws ExecException
     *             if the field number given is greater than or equal to the number of fields in the tuple.
     */
    @Override
    public boolean isNull(int fieldNum) throws ExecException {
        return (get(fieldNum) == null);
    }

    @Override
    @Deprecated
    public boolean isNull() {
        return false;
    }

    @Override
    @Deprecated
    public void setNull(boolean isNull) {
    }

    @Override
    public boolean equals(Object other) {
        return (compareTo(other) == 0);
    }
}
