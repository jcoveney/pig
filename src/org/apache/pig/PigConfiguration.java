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

package org.apache.pig;

/**
 * Container for static configuration strings, defaults, etc.
 */
public class PigConfiguration {
    private PigConfiguration() {}

    /////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////       COMMAND LINE KEYS       /////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////

    /**
     * Controls whether execution time of Pig UDFs should be tracked.
     * This feature uses counters; use judiciously.
     */
    public static final String TIME_UDFS_PROP = "pig.udf.profile";

    /**
     * This key must be set to true by the user for code generation to be used.
     * In the future, it may be turned on by default (at least in certain cases),
     * but for now it is too experimental.
     */
    public static final String SHOULD_USE_SCHEMA_TUPLE = "pig.schematuple";

    public static final String SCHEMA_TUPLE_SHOULD_USE_IN_UDF = "pig.schematuple.udf";

    public static final String SCHEMA_TUPLE_SHOULD_USE_IN_FOREACH = "pig.schematuple.foreach";

    public static final String SCHEMA_TUPLE_SHOULD_USE_IN_FRJOIN = "pig.schematuple.fr_join";

    public static final String SCHEMA_TUPLE_SHOULD_USE_IN_MERGEJOIN = "pig.schematuple.merge_join";

    public static final String SCHEMA_TUPLE_SHOULD_ALLOW_FORCE = "pig.schematuple.force";

    /////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////       JOB CONF KEYS       /////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////

    /**
     * This key is used in the job conf to let the various jobs know what code was
     * generated.
     */
    public static final String GENERATED_CLASSES_KEY = "pig.schematuple.classes";

    /////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////         DEFAULTS          /////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////

    public static final String SCHEMA_TUPLE_ON_BY_DEFAULT = "true";
}
