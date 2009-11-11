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

import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.hbql.client.Util;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Random;

public class RecordAllTypes implements Serializable {

    HRecord rec;

    public static class TestObject implements Serializable {
        public int intvalue = random.nextInt();

        public boolean equals(final Object o) {

            if (!(o instanceof TestObject))
                return false;

            final TestObject val = (TestObject)o;

            return val.intvalue == this.intvalue;
        }
    }

    public RecordAllTypes() throws HBqlException {
        this.rec = SchemaManager.newRecord("alltypes");
    }

    static Random random = new Random();

    public void setSomeValues(int val, boolean noRandomData, int cnt) throws HBqlException {

        this.rec.setCurrentValue("keyval", Util.getZeroPaddedNumber(val, 10));

        if (noRandomData || random.nextBoolean()) {
            this.rec.setCurrentValue("booleanValue", random.nextBoolean());
            boolean[] array = new boolean[cnt];
            for (int i = 0; i < cnt; i++)
                array[i] = random.nextBoolean();
            this.rec.setCurrentValue("booleanArrayValue", array);
        }

        if (noRandomData || random.nextBoolean()) {
            this.rec.setCurrentValue("byteValue", (byte)random.nextInt());
            byte[] array = new byte[cnt];
            for (int i = 0; i < cnt; i++)
                array[i] = (byte)random.nextInt();
            this.rec.setCurrentValue("byteArrayValue", array);
        }

        if (noRandomData || random.nextBoolean()) {
            this.rec.setCurrentValue("charValue", ("" + random.nextInt()).charAt(0));
            char[] array = ("" + random.nextInt()).toCharArray();
            this.rec.setCurrentValue("charArrayValue", array);
        }

        if (noRandomData || random.nextBoolean()) {
            this.rec.setCurrentValue("shortValue", (short)random.nextInt());
            short[] array = new short[cnt];
            for (int i = 0; i < cnt; i++)
                array[i] = (short)random.nextInt();
            this.rec.setCurrentValue("shortArrayValue", array);
        }

        if (noRandomData || random.nextBoolean()) {
            this.rec.setCurrentValue("intValue", random.nextInt());
            int[] array = new int[cnt];
            for (int i = 0; i < cnt; i++)
                array[i] = random.nextInt();
            this.rec.setCurrentValue("intArrayValue", array);
        }

        if (noRandomData || random.nextBoolean()) {
            this.rec.setCurrentValue("longValue", random.nextLong());
            long[] array = new long[cnt];
            for (int i = 0; i < cnt; i++)
                array[i] = random.nextLong();
            this.rec.setCurrentValue("longArrayValue", array);
        }

        if (noRandomData || random.nextBoolean()) {
            this.rec.setCurrentValue("floatValue", random.nextFloat());
            float[] array = new float[cnt];
            for (int i = 0; i < cnt; i++)
                array[i] = random.nextFloat();
            this.rec.setCurrentValue("floatArrayValue", array);
        }

        if (noRandomData || random.nextBoolean()) {
            this.rec.setCurrentValue("doubleValue", random.nextDouble());
            double[] array = new double[cnt];
            for (int i = 0; i < cnt; i++)
                array[i] = random.nextDouble();
            this.rec.setCurrentValue("doubleArrayValue", array);
        }

        if (noRandomData || random.nextBoolean()) {
            this.rec.setCurrentValue("stringValue", "" + random.nextDouble());
            String[] array = new String[cnt];
            for (int i = 0; i < cnt; i++)
                array[i] = "" + random.nextDouble();
            this.rec.setCurrentValue("stringArrayValue", array);
        }

        if (noRandomData || random.nextBoolean()) {
            this.rec.setCurrentValue("dateValue", new Date(random.nextLong()));
            Date[] array = new Date[cnt];
            for (int i = 0; i < cnt; i++)
                array[i] = new Date(random.nextLong());
            this.rec.setCurrentValue("dateArrayValue", array);
        }

        if (noRandomData || random.nextBoolean()) {
            this.rec.setCurrentValue("mapValue", getRandomMap(cnt));
            Map[] array = new Map[cnt];
            for (int i = 0; i < cnt; i++)
                array[i] = getRandomMap(cnt);
            this.rec.setCurrentValue("mapArrayValue", array);
        }

        if (noRandomData || random.nextBoolean()) {
            this.rec.setCurrentValue("objectValue", new TestObject());
            TestObject[] array = new TestObject[cnt];
            for (int i = 0; i < cnt; i++)
                array[i] = new TestObject();
            this.rec.setCurrentValue("objectArrayValue", array);
        }
    }

    private Map<String, String> getRandomMap(int cnt) {
        Map<String, String> retval = Maps.newHashMap();

        for (int i = 0; i < cnt; i++)
            retval.put("" + random.nextInt(), "" + random.nextDouble());

        return retval;
    }

    public HRecord getRec() {
        return rec;
    }

    public boolean equals(final Object o) {

        if (!(o instanceof RecordAllTypes))
            return false;

        final RecordAllTypes val = (RecordAllTypes)o;

        try {
            return val.getRec().getCurrentValue("keyval").equals(this.getRec().getCurrentValue("keyval"))
                   && val.getRec().getCurrentValue("booleanValue") == this.getRec().getCurrentValue("booleanValue")

                   && Arrays.equals((boolean[])val.getRec().getCurrentValue("booleanArrayValue"),
                                    (boolean[])this.getRec().getCurrentValue("booleanArrayValue"))

                   && val.getRec().getCurrentValue("byteValue") == this.getRec().getCurrentValue("byteValue")
                   && Arrays.equals((byte[])val.getRec().getCurrentValue("byteArrayValue"),
                                    (byte[])this.getRec().getCurrentValue("byteArrayValue"))

                   && val.getRec().getCurrentValue("charValue") == this.getRec().getCurrentValue("charValue")
                   && Arrays.equals((char[])val.getRec().getCurrentValue("charArrayValue"),
                                    (char[])this.getRec().getCurrentValue("charArrayValue"))

                   && val.getRec().getCurrentValue("shortValue") == this.getRec().getCurrentValue("shortValue")
                   && Arrays.equals((short[])val.getRec().getCurrentValue("shortArrayValue"),
                                    (short[])this.getRec().getCurrentValue("shortArrayValue"))

                   && val.getRec().getCurrentValue("intValue") == this.getRec().getCurrentValue("intValue")
                   && Arrays.equals((int[])val.getRec().getCurrentValue("intArrayValue"),
                                    (int[])this.getRec().getCurrentValue("intArrayValue"))

                   && val.getRec().getCurrentValue("longValue") == this.getRec().getCurrentValue("longValue")
                   && Arrays.equals((long[])val.getRec().getCurrentValue("longArrayValue"),
                                    (long[])this.getRec().getCurrentValue("longArrayValue"))

                   && val.getRec().getCurrentValue("floatValue") == this.getRec().getCurrentValue("floatValue")
                   && Arrays.equals((float[])val.getRec().getCurrentValue("floatArrayValue"),
                                    (float[])this.getRec().getCurrentValue("floatArrayValue"))

                   && val.getRec().getCurrentValue("doubleValue") == this.getRec().getCurrentValue("doubleValue")
                   && Arrays.equals((double[])val.getRec().getCurrentValue("doubleArrayValue"),
                                    (double[])this.getRec().getCurrentValue("doubleArrayValue"))

                   && ((val.getRec().getCurrentValue("stringValue") == null
                        && this.getRec().getCurrentValue("stringValue") == null)
                       || val.getRec().getCurrentValue("stringValue")
                    .equals(this.getRec().getCurrentValue("stringValue")))

                   && ((val.getRec().getCurrentValue("stringArrayValue") == null
                        && this.getRec().getCurrentValue("stringArrayValue") == null)
                       || Arrays.equals((String[])val.getRec().getCurrentValue("stringArrayValue"),
                                        (String[])this.getRec().getCurrentValue("stringArrayValue")))

                   && ((val.getRec().getCurrentValue("dateValue") == null && this.getRec()
                    .getCurrentValue("dateValue") == null)
                       || val.getRec().getCurrentValue("dateValue").equals(this.getRec().getCurrentValue("dateValue")))

                   && ((val.getRec().getCurrentValue("dateArrayValue") == null && this.getRec()
                    .getCurrentValue("dateArrayValue") == null)
                       || Arrays.equals((Date[])val.getRec().getCurrentValue("dateArrayValue"),
                                        (Date[])this.getRec()
                                                .getCurrentValue("dateArrayValue")))

                   && ((val.getRec().getCurrentValue("mapValue") == null
                        && this.getRec().getCurrentValue("mapValue") == null)
                       || val.getRec().getCurrentValue("mapValue").equals(this.getRec().getCurrentValue("mapValue")))

                   && ((val.getRec().getCurrentValue("mapArrayValue") == null && this.getRec()
                    .getCurrentValue("mapArrayValue") == null)
                       || Arrays.equals((Map[])val.getRec().getCurrentValue("mapArrayValue"),
                                        (Map[])this.getRec().getCurrentValue("mapArrayValue")))

                   && ((val.getRec().getCurrentValue("objectValue") == null
                        && this.getRec().getCurrentValue("objectValue") == null)
                       || val.getRec().getCurrentValue("objectValue")
                    .equals(this.getRec().getCurrentValue("objectValue")))
                   && ((val.getRec().getCurrentValue("objectArrayValue") == null
                        && this.getRec().getCurrentValue("objectArrayValue") == null)
                       || Arrays.equals((Object[])val.getRec().getCurrentValue("objectArrayValue"),
                                        (Object[])this.getRec().getCurrentValue("objectArrayValue")))
                    ;
        }
        catch (HBqlException e) {
            e.printStackTrace();
            return false;
        }
    }
}