/*
 * Copyright (c) 2009.  The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.hbql;

import org.apache.hadoop.hbase.hbql.client.HBatch;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HConnectionManager;
import org.apache.hadoop.hbase.hbql.client.HPreparedStatement;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HResultSet;
import org.apache.hadoop.hbase.hbql.client.HStatement;
import org.apache.hadoop.hbase.hbql.util.Lists;
import org.apache.hadoop.hbase.hbql.util.TestSupport;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class RecordAllTypesTest extends TestSupport {

    static HConnection connection = null;

    static int cnt = 10;

    @BeforeClass
    public static void beforeClass() throws HBqlException {

        connection = HConnectionManager.newConnection();

        connection.execute("CREATE TEMP MAPPING alltypes FOR TABLE example2"
                           + "("
                           + "keyval KEY, "
                           + "f1 ("
                           + "  val1 boolean ALIAS booleanValue, "
                           + "  val2 boolean[] ALIAS booleanArrayValue, "
                           + "  val3 byte ALIAS byteValue, "
                           + "  val4 byte[] ALIAS byteArrayValue, "
                           + "  val5 char ALIAS charValue, "
                           + "  val6 char[] ALIAS charArrayValue, "
                           + "  val7 short ALIAS shortValue, "
                           + "  val8 short[] ALIAS shortArrayValue, "
                           + "  val9 int ALIAS intValue, "
                           + "  val10 int[] ALIAS intArrayValue, "
                           + "  val11 long ALIAS longValue, "
                           + "  val12 long[] ALIAS longArrayValue, "
                           + "  val13 float ALIAS floatValue, "
                           + "  val14 float[] ALIAS floatArrayValue, "
                           + "  val15 double ALIAS doubleValue, "
                           + "  val16 double[] ALIAS doubleArrayValue, "
                           + "  val17 string ALIAS stringValue, "
                           + "  val18 string[] ALIAS stringArrayValue, "
                           + "  val19 date ALIAS dateValue, "
                           + "  val20 date[] ALIAS dateArrayValue, "
                           + "  val21 object ALIAS mapValue, "
                           + "  val22 object[] ALIAS mapArrayValue, "
                           + "  val23 object ALIAS objectValue, "
                           + "  val24 object[] ALIAS objectArrayValue "
                           + "))");

        if (!connection.tableExists("example2"))
            System.out.println(connection.execute("create table example2 (f1() )"));
        else {
            System.out.println(connection.execute("delete from alltypes"));
        }
    }

    public static List<RecordAllTypes> insertSomeData(int cnt, boolean noRandomData) throws HBqlException {

        List<RecordAllTypes> retval = Lists.newArrayList();
        final HBatch<HRecord> batch = HBatch.newHBatch(connection);

        for (int i = 0; i < cnt; i++) {

            RecordAllTypes rat = new RecordAllTypes();
            rat.setSomeValues(i, noRandomData, cnt);

            retval.add(rat);

            batch.insert(rat.getHRecord(connection));
        }

        batch.apply();

        return retval;
    }

    @Test
    public void simpleSelect() throws HBqlException {

        List<RecordAllTypes> vals = insertSomeData(cnt, true);

        assertTrue(vals.size() == cnt);

        HStatement stmt = connection.createStatement();
        HResultSet<HRecord> recs = stmt.executeQuery("select * from alltypes");

        int reccnt = 0;
        for (final HRecord rec : recs)
            assertTrue((new RecordAllTypes(rec)).equals(vals.get(reccnt++)));

        assertTrue(reccnt == cnt);
    }

    @Test
    public void simpleSparseSelect() throws HBqlException {

        List<RecordAllTypes> vals = insertSomeData(cnt, false);

        assertTrue(vals.size() == cnt);

        HStatement stmt = connection.createStatement();
        HResultSet<HRecord> recs = stmt.executeQuery("select * from alltypes");

        int reccnt = 0;
        for (final HRecord rec : recs)
            assertTrue((new RecordAllTypes(rec)).equals(vals.get(reccnt++)));

        assertTrue(reccnt == cnt);
    }

    @Test
    public void simpleLimitSelect() throws HBqlException {

        List<RecordAllTypes> vals = insertSomeData(cnt, true);

        assertTrue(vals.size() == cnt);

        HPreparedStatement stmt = connection.prepareStatement("select * from alltypes WITH LIMIT :limit");

        stmt.setParameter("limit", cnt / 2);
        HResultSet<HRecord> recs = stmt.executeQuery();

        int reccnt = 0;
        for (final HRecord rec : recs)
            assertTrue((new RecordAllTypes(rec)).equals(vals.get(reccnt++)));

        assertTrue(reccnt == cnt / 2);
    }
}