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
package org.apache.pig.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.ExecType;
import org.apache.pig.PigException;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.tools.grunt.GruntParser;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestMultiQueryLocal {

    private PigServer myPig;

    @Before
    public void setUp() throws Exception {
        PigContext context = new PigContext(ExecType.LOCAL, new Properties());
        context.getProperties().setProperty("opt.multiquery", ""+true);
        myPig = new PigServer(context);
        myPig.getPigContext().getProperties().setProperty("pig.usenewlogicalplan", "false");
        deleteOutputFiles();
    }

    @After
    public void tearDown() throws Exception {
        myPig = null;
    }

    @Test
    public void testMultiQueryWithTwoStores() throws Exception {

        System.out.println("===== test multi-query with 2 stores =====");


        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'test/org/apache/pig/test/data/passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = filter a by uid > 5;");
            myPig.registerQuery("store b into '/tmp/Pig-TestMultiQueryLocal1';");
            myPig.registerQuery("c = group b by gid;");
            myPig.registerQuery("store c into '/tmp/Pig-TestMultiQueryLocal2';");

            LogicalPlan lp = checkLogicalPlan(1, 2, 5);

            // XXX Physical plan has one less node in the local case
            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 2, 11);

            assertTrue(executePlan(pp));
        } finally {
            deleteOutputFiles();
        }
    }

    @Test
    public void testEmptyExecute() throws Exception {
        System.out.println("=== test empty execute ===");

        myPig.setBatchOn();
        myPig.executeBatch();
        myPig.executeBatch();
        myPig.discardBatch();
    }

    @Test
    public void testMultiQueryWithTwoStores2() throws Exception {

        System.out.println("===== test multi-query with 2 stores (2) =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'test/org/apache/pig/test/data/passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = filter a by uid > 5;");
            myPig.registerQuery("store b into '/tmp/Pig-TestMultiQueryLocal1';");
            myPig.registerQuery("c = group b by gid;");
            myPig.registerQuery("store c into '/tmp/Pig-TestMultiQueryLocal2';");

            myPig.executeBatch();
        } finally {
            deleteOutputFiles();
        }
    }

    @Test
    public void testMultiQueryWithTwoStores2Execs() throws Exception {

        System.out.println("===== test multi-query with 2 stores (2) =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'test/org/apache/pig/test/data/passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = filter a by uid > 5;");
            myPig.executeBatch();
            myPig.registerQuery("store b into '/tmp/Pig-TestMultiQueryLocal1';");
            myPig.executeBatch();
            myPig.registerQuery("c = group b by gid;");
            myPig.registerQuery("store c into '/tmp/Pig-TestMultiQueryLocal2';");

            myPig.executeBatch();
            myPig.discardBatch();
        } finally {
            deleteOutputFiles();
        }
    }

    @Test
    public void testMultiQueryWithThreeStores() throws Exception {

        System.out.println("===== test multi-query with 3 stores =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'test/org/apache/pig/test/data/passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = filter a by uid > 5;");
            myPig.registerQuery("store b into '/tmp/Pig-TestMultiQueryLocal1';");
            myPig.registerQuery("c = filter b by uid > 10;");
            myPig.registerQuery("store c into '/tmp/Pig-TestMultiQueryLocal2';");
            myPig.registerQuery("d = filter c by uid > 15;");
            myPig.registerQuery("store d into '/tmp/Pig-TestMultiQueryLocal3';");

            LogicalPlan lp = checkLogicalPlan(1, 3, 7);

            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 3, 14);

            assertTrue(executePlan(pp));
        } finally {
            deleteOutputFiles();
        }
    }

    @Test
    public void testMultiQueryWithThreeStores2() throws Exception {

        System.out.println("===== test multi-query with 3 stores (2) =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'test/org/apache/pig/test/data/passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = filter a by uid > 5;");
            myPig.registerQuery("store b into '/tmp/Pig-TestMultiQueryLocal1';");
            myPig.registerQuery("c = filter b by uid > 10;");
            myPig.registerQuery("store c into '/tmp/Pig-TestMultiQueryLocal2';");
            myPig.registerQuery("d = filter c by uid > 15;");
            myPig.registerQuery("store d into '/tmp/Pig-TestMultiQueryLocal3';");

            myPig.executeBatch();
            myPig.discardBatch();
        } finally {
            deleteOutputFiles();
        }
    }

    @Test
    public void testMultiQueryWithTwoLoads() throws Exception {

        System.out.println("===== test multi-query with two loads =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'test/org/apache/pig/test/data/passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = load 'test/org/apache/pig/test/data/passwd2' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("c = filter a by uid > 5;");
            myPig.registerQuery("d = filter b by uid > 10;");
            myPig.registerQuery("store c into '/tmp/Pig-TestMultiQueryLocal1';");
            myPig.registerQuery("store d into '/tmp/Pig-TestMultiQueryLocal2';");
            myPig.registerQuery("e = cogroup c by uid, d by uid;");
            myPig.registerQuery("store e into '/tmp/Pig-TestMultiQueryLocal3';");

            LogicalPlan lp = checkLogicalPlan(2, 3, 8);

            // XXX the total number of ops is one less in the local case
            PhysicalPlan pp = checkPhysicalPlan(lp, 2, 3, 19);

            assertTrue(executePlan(pp));
        } finally {
            deleteOutputFiles();
        }
    }

    @Test
    public void testMultiQueryWithTwoLoads2() throws Exception {

        System.out.println("===== test multi-query with two loads (2) =====");

        try {
            myPig.setBatchOn();

            myPig.registerQuery("a = load 'test/org/apache/pig/test/data/passwd' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("b = load 'test/org/apache/pig/test/data/passwd2' " +
                                "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
            myPig.registerQuery("c = filter a by uid > 5;");
            myPig.registerQuery("d = filter b by uid > 10;");
            myPig.registerQuery("store c into '/tmp/Pig-TestMultiQueryLocal1';");
            myPig.registerQuery("store d into '/tmp/Pig-TestMultiQueryLocal2';");
            myPig.registerQuery("e = cogroup c by uid, d by uid;");
            myPig.registerQuery("store e into '/tmp/Pig-TestMultiQueryLocal3';");

            myPig.executeBatch();
            myPig.discardBatch();
        } finally {
            deleteOutputFiles();
        }
    }

    @Test
    public void testMultiQueryWithNoStore() throws Exception {

        System.out.println("===== test multi-query with no store =====");

        myPig.setBatchOn();

        myPig.registerQuery("a = load 'test/org/apache/pig/test/data/passwd' " +
                            "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
        myPig.registerQuery("b = filter a by uid > 5;");
        myPig.registerQuery("group b by gid;");

        LogicalPlan lp = checkLogicalPlan(0, 0, 0);

        // XXX Physical plan has one less node in the local case
        PhysicalPlan pp = checkPhysicalPlan(lp, 0, 0, 0);
    }

    @Test
    public void testMultiQueryWithNoStore2() throws Exception {

        System.out.println("===== test multi-query with no store (2) =====");

        myPig.setBatchOn();

        myPig.registerQuery("a = load 'test/org/apache/pig/test/data/passwd' " +
                            "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
        myPig.registerQuery("b = filter a by uid > 5;");
        myPig.registerQuery("group b by gid;");

        myPig.executeBatch();
        myPig.discardBatch();
    }

    public static class PigStorageWithSuffix extends PigStorage {

        private String suffix;
        public PigStorageWithSuffix(String s) {
            this.suffix = s;
        }
        static private final String key="test.key";
        @Override
        public void setStoreLocation(String location, Job job) throws IOException {
            super.setStoreLocation(location, job);
            if (job.getConfiguration().get(key)==null) {
                job.getConfiguration().set(key, suffix);
            }
            suffix = job.getConfiguration().get(key);
        }

        @Override
        public void putNext(Tuple f) throws IOException {
            try {
                Tuple t = TupleFactory.getInstance().newTuple();
                for (Object obj : f.getAll()) {
                    t.append(obj);
                }
                t.append(suffix);
                writer.write(null, t);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
    }

    // See PIG-2578
    @Test
    public void testMultiStoreWithConfig() throws Exception {

        System.out.println("===== test multi-query with competing config =====");

        myPig.setBatchOn();

        myPig.registerQuery("a = load 'test/org/apache/pig/test/data/passwd' " +
                            "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);");
        myPig.registerQuery("store a into '/tmp/Pig-TestMultiQueryLocal1' using " + PigStorageWithSuffix.class.getName() + "('a');");
        myPig.registerQuery("store a into '/tmp/Pig-TestMultiQueryLocal2' using " + PigStorageWithSuffix.class.getName() + "('b');");

        myPig.executeBatch();
        myPig.discardBatch();
        BufferedReader reader = new BufferedReader(new FileReader("/tmp/Pig-TestMultiQueryLocal1/part-m-00000"));
        String line;
        while ((line = reader.readLine())!=null) {
            assertTrue(line.endsWith("a"));
        }
        reader = new BufferedReader(new FileReader("/tmp/Pig-TestMultiQueryLocal2/part-m-00000"));
        while ((line = reader.readLine())!=null) {
            assertTrue(line.endsWith("b"));
        }
    }

    @Test
    public void testMultiQueryWithExplain() throws Exception {

        System.out.println("===== test multi-query with explain =====");

        try {
            String script = "a = load 'test/org/apache/pig/test/data/passwd' "
                          + "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);"
                          + "b = filter a by uid > 5;"
                          + "explain b;"
                          + "store b into '/tmp/Pig-TestMultiQueryLocal1';\n";

            GruntParser parser = new GruntParser(new StringReader(script));
            parser.setInteractive(false);
            parser.setParams(myPig);
            parser.parseStopOnError();
        } finally {
            deleteOutputFiles();
        }
    }

    @Test
    public void testMultiQueryWithDump() throws Exception {

        System.out.println("===== test multi-query with dump =====");

        try {
            String script = "a = load 'test/org/apache/pig/test/data/passwd' "
                          + "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);"
                          + "b = filter a by uid > 5;"
                          + "dump b;"
                          + "store b into '/tmp/Pig-TestMultiQueryLocal1';\n";

            GruntParser parser = new GruntParser(new StringReader(script));
            parser.setInteractive(false);
            parser.setParams(myPig);
            parser.parseStopOnError();
        } finally {
            deleteOutputFiles();
        }
    }

    @Test
    public void testMultiQueryWithDescribe() throws Exception {

        System.out.println("===== test multi-query with describe =====");

        try {
            String script = "a = load 'test/org/apache/pig/test/data/passwd' "
                          + "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);"
                          + "b = filter a by uid > 5;"
                          + "describe b;"
                          + "store b into '/tmp/Pig-TestMultiQueryLocal1';\n";

            GruntParser parser = new GruntParser(new StringReader(script));
            parser.setInteractive(false);
            parser.setParams(myPig);
            parser.parseStopOnError();
        } finally {
            deleteOutputFiles();
        }
    }

    @Test
    public void testMultiQueryWithIllustrate() throws Exception {

        System.out.println("===== test multi-query with illustrate =====");

        try {
            String script = "a = load 'test/org/apache/pig/test/data/passwd' "
                          + "using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int,gid:int);"
                          + "b = filter a by uid > 5;"
                          + "illustrate b;"
                          + "store b into '/tmp/Pig-TestMultiQueryLocal1';\n";

            GruntParser parser = new GruntParser(new StringReader(script));
            parser.setInteractive(false);
            myPig.getPigContext().getProperties().setProperty("pig.usenewlogicalplan", "true");
            parser.setParams(myPig);
            parser.parseStopOnError();
        } finally {
            deleteOutputFiles();
            myPig.getPigContext().getProperties().setProperty("pig.usenewlogicalplan", "false");
        }
    }

    @Test
    public void testStoreOrder() throws Exception {
        System.out.println("===== multi-query store order =====");

        try {
            myPig.setBatchOn();
            myPig.registerQuery("a = load 'test/org/apache/pig/test/data/passwd';");
            myPig.registerQuery("store a into '/tmp/Pig-TestMultiQueryLocal1' using BinStorage();");
            myPig.registerQuery("a = load '/tmp/Pig-TestMultiQueryLocal1';");
            myPig.registerQuery("store a into '/tmp/Pig-TestMultiQueryLocal2';");
            myPig.registerQuery("a = load '/tmp/Pig-TestMultiQueryLocal1';");
            myPig.registerQuery("store a into '/tmp/Pig-TestMultiQueryLocal3';");
            myPig.registerQuery("a = load '/tmp/Pig-TestMultiQueryLocal2' using BinStorage();");
            myPig.registerQuery("store a into '/tmp/Pig-TestMultiQueryLocal4';");
            myPig.registerQuery("a = load '/tmp/Pig-TestMultiQueryLocal2';");
            myPig.registerQuery("b = load '/tmp/Pig-TestMultiQueryLocal1';");
            myPig.registerQuery("c = cogroup a by $0, b by $0;");
            myPig.registerQuery("store c into '/tmp/Pig-TestMultiQueryLocal5';");

            LogicalPlan lp = checkLogicalPlan(1, 3, 12);
            PhysicalPlan pp = checkPhysicalPlan(lp, 1, 3, 15);

            myPig.executeBatch();
            myPig.discardBatch();

            assertTrue(new File("/tmp/Pig-TestMultiQueryLocal1").exists());
            assertTrue(new File("/tmp/Pig-TestMultiQueryLocal2").exists());
            assertTrue(new File("/tmp/Pig-TestMultiQueryLocal3").exists());
            assertTrue(new File("/tmp/Pig-TestMultiQueryLocal4").exists());
            assertTrue(new File("/tmp/Pig-TestMultiQueryLocal5").exists());
        } finally {
            deleteOutputFiles();
        }
    }

    // --------------------------------------------------------------------------
    // Helper methods

    private <T extends OperatorPlan<? extends Operator<?>>>
    void showPlanOperators(T p) {
        System.out.println("Operators:");

        ArrayList<Operator<?>> ops = new ArrayList<Operator<?>>(p.getKeys()
                .values());
        Collections.sort(ops);
        for (Operator<?> op : ops) {
            System.out.println("    op: " + op.name());
        }
        System.out.println();
    }

    private LogicalPlan checkLogicalPlan(int expectedRoots,
            int expectedLeaves, int expectedSize) throws Exception {

        System.out.println("===== check logical plan =====");

        LogicalPlan lp = null;

        try {
            java.lang.reflect.Method buildLp = myPig.getClass().getDeclaredMethod("buildLp");
            buildLp.setAccessible(true);
            lp = (LogicalPlan) buildLp.invoke( myPig );

            assertNotNull(lp);

        } catch (Exception e) {
            PigException pe = LogUtils.getPigException(e);
            if (pe != null) {
                throw pe;
            } else {
                throw e;
            }
        }

        assertEquals(expectedRoots, lp.getSources().size());
        assertEquals(expectedLeaves, lp.getSinks().size());
        assertEquals(expectedSize, lp.size());

        TestMultiQueryCompiler.showLPOperators(lp);

        return lp;
    }

    private PhysicalPlan checkPhysicalPlan(LogicalPlan lp, int expectedRoots,
            int expectedLeaves, int expectedSize) throws IOException {

        System.out.println("===== check physical plan =====");

        PhysicalPlan pp = myPig.getPigContext().getExecutionEngine().compile(
                lp, null);

        assertEquals(expectedRoots, pp.getRoots().size());
        assertEquals(expectedLeaves, pp.getLeaves().size());
        assertEquals(expectedSize, pp.size());

        showPlanOperators(pp);

        return pp;
    }

    private boolean executePlan(PhysicalPlan pp) throws IOException {
        boolean failed = true;
        MapReduceLauncher launcher = new MapReduceLauncher();
        PigStats stats = null;
        try {
            stats = launcher.launchPig(pp, "execute", myPig.getPigContext());
        } catch (Exception e) {
            e.printStackTrace(System.out);
            throw new IOException(e);
        }
        Iterator<JobStats> iter = stats.getJobGraph().iterator();
        while (iter.hasNext()) {
            JobStats js = iter.next();
            failed = !js.isSuccessful();
            if (failed) {
                break;
            }
        }
        return !failed;
    }

    private void deleteOutputFiles() throws Exception {
        String outputFiles[] = { "/tmp/Pig-TestMultiQueryLocal1",
                                 "/tmp/Pig-TestMultiQueryLocal2",
                                 "/tmp/Pig-TestMultiQueryLocal3",
                                 "/tmp/Pig-TestMultiQueryLocal4",
                                 "/tmp/Pig-TestMultiQueryLocal5"
                };
        for( String outputFile : outputFiles ) {
            if( isDirectory(outputFile) ) {
                deleteDir( new File( outputFile ) );
            } else {
                FileLocalizer.delete(outputFile, myPig.getPigContext());
            }
        }
    }

    private void deleteDir( File file ) {
        if( file.isDirectory() && file.listFiles().length != 0 ) {
            for( File innerFile : file.listFiles() ) {
                deleteDir( innerFile );
            }
        }
        file.delete();
    }

    private boolean isDirectory( String filepath ) {
        File file = new File( filepath );
        return file.isDirectory();
    }
}