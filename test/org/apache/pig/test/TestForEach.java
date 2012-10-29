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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.utils.GenPhyOp;
import org.apache.pig.test.utils.GenRandomData;
import org.apache.pig.test.utils.TestHelper;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests foreach db generate $0
 * Constructs a bag that projects the input
 * bag onto the the column $0 and checks if
 * the tuples generated by foreach are the same
 * as those in the projected bag
 *
 */
public class TestForEach {
    POForEach fe;
    Tuple t;
    DataBag db;
    DataBag projDB;

    @Before
    public void setUp() throws Exception {
        Random r = new Random();
        db = GenRandomData.genRandSmallTupDataBagWithNulls(r, 10, 100);
        projDB = TestHelper.projectBag(db,0);
        fe = GenPhyOp.topForEachOPWithPlan(0,db.iterator().next());
        POProject proj = GenPhyOp.exprProject();
        proj.setColumn(0);
        proj.setResultType(DataType.TUPLE);
        proj.setOverloaded(true);
        Tuple t = new DefaultTuple();
        t.append(db);
        proj.attachInput(t);
        List<PhysicalOperator> inputs = new ArrayList<PhysicalOperator>();
        inputs.add(proj);
        fe.setInputs(inputs);
    }

    @Test
    public void testGetNextTuple() throws ExecException, IOException {
        int size=0;
        for (Result res = fe.getNext(t); res.returnStatus != POStatus.STATUS_EOP; res = fe.getNext(t)){
            Tuple t = (Tuple)res.result;
            assertTrue( TestHelper.bagContains(projDB, t));
            ++size;
        }
        assertEquals(size, db.size());
    }
}