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
import org.apache.hadoop.hbase.hbql.client.Record;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.hbql.client.Util;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Random;

public class RecordAllTypes implements Serializable {

    Record rec;

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
            this.byteArrayValue = new byte[cnt];
            for (int i = 0; i < cnt; i++)
                this.byteArrayValue[i] = (byte)random.nextInt();
            this.rec.setCurrentValue("byteValue", array);
        }

        if (noRandomData || random.nextBoolean()) {
            this.rec.setCurrentValue("charValue", ("" + random.nextInt()).charAt(0));
            this.charArrayValue = ("" + random.nextInt()).toCharArray();
            this.rec.setCurrentValue("charValue", array);
        }

        if (noRandomData || random.nextBoolean()) {
            this.rec.setCurrentValue("shortValue", (short)random.nextInt());
            this.shortArrayValue = new short[cnt];
            for (int i = 0; i < cnt; i++)
                this.shortArrayValue[i] = (short)random.nextInt();
            this.rec.setCurrentValue("shortValue", array);
        }

        if (noRandomData || random.nextBoolean()) {
            this.rec.setCurrentValue("intValue", random.nextInt());
            this.intArrayValue = new int[cnt];
            for (int i = 0; i < cnt; i++)
                this.intArrayValue[i] = random.nextInt();
            this.rec.setCurrentValue("booleanArrayValue", array);
        }

        if (noRandomData || random.nextBoolean()) {
            this.rec.setCurrentValue("longValue", random.nextLong());
            this.longArrayValue = new long[cnt];
            for (int i = 0; i < cnt; i++)
                this.longArrayValue[i] = random.nextLong();
            this.rec.setCurrentValue("booleanArrayValue", array);
        }

        if (noRandomData || random.nextBoolean()) {
            this.rec.setCurrentValue("floatValue", random.nextFloat());
            this.floatArrayValue = new float[cnt];
            for (int i = 0; i < cnt; i++)
                this.floatArrayValue[i] = random.nextFloat();
            this.rec.setCurrentValue("booleanArrayValue", array);
        }

        if (noRandomData || random.nextBoolean()) {
            this.rec.setCurrentValue("doubleValue", random.nextDouble());
            this.doubleArrayValue = new double[cnt];
            for (int i = 0; i < cnt; i++)
                this.doubleArrayValue[i] = random.nextDouble();
            this.rec.setCurrentValue("booleanArrayValue", array);
        }

        if (noRandomData || random.nextBoolean()) {
            this.rec.setCurrentValue("stringValue", "" + random.nextDouble());
            this.stringArrayValue = new String[cnt];
            for (int i = 0; i < cnt; i++)
                this.stringArrayValue[i] = "" + random.nextDouble();
            this.rec.setCurrentValue("booleanArrayValue", array);
        }

        if (noRandomData || random.nextBoolean()) {
            this.rec.setCurrentValue("dateValue", new Date(random.nextLong()));
            this.dateArrayValue = new Date[cnt];
            for (int i = 0; i < cnt; i++)
                this.dateArrayValue[i] = new Date(random.nextLong());
            this.rec.setCurrentValue("booleanArrayValue", array);
        }

        if (noRandomData || random.nextBoolean()) {
            this.rec.setCurrentValue("mapValue", getRandomMap(cnt));
            this.mapArrayValue = new Map[cnt];
            for (int i = 0; i < cnt; i++)
                this.mapArrayValue[i] = getRandomMap(cnt);
            this.rec.setCurrentValue("booleanArrayValue", array);
        }

        if (noRandomData || random.nextBoolean()) {
            this.rec.setCurrentValue("objectValue", new TestObject());
            this.objectArrayValue = new TestObject[cnt];
            for (int i = 0; i < cnt; i++)
                this.objectArrayValue[i] = new TestObject();
            this.rec.setCurrentValue("booleanArrayValue", array);
        }
    }

    private Map<String, String> getRandomMap(int cnt) {
        Map<String, String> retval = Maps.newHashMap();

        for (int i = 0; i < cnt; i++)
            retval.put("" + random.nextInt(), "" + random.nextDouble());

        return retval;
    }

    public boolean equals(final Object o) {

        if (!(o instanceof RecordAllTypes))
            return false;

        final RecordAllTypes val = (RecordAllTypes)o;

        return val.keyval.equals(this.keyval)
               && val.booleanValue == this.booleanValue
               && Arrays.equals(val.booleanArrayValue, this.booleanArrayValue)
               && val.byteValue == this.byteValue
               && Arrays.equals(val.byteArrayValue, this.byteArrayValue)
               && val.charValue == this.charValue
               && Arrays.equals(val.charArrayValue, this.charArrayValue)
               && val.shortValue == this.shortValue
               && Arrays.equals(val.shortArrayValue, this.shortArrayValue)
               && val.intValue == this.intValue
               && Arrays.equals(val.intArrayValue, this.intArrayValue)
               && val.longValue == this.longValue
               && Arrays.equals(val.longArrayValue, this.longArrayValue)
               && val.floatValue == this.floatValue
               && Arrays.equals(val.floatArrayValue, this.floatArrayValue)
               && val.doubleValue == this.doubleValue
               && Arrays.equals(val.doubleArrayValue, this.doubleArrayValue)

               && ((val.stringValue == null && this.stringValue == null)
                   || val.stringValue.equals(this.stringValue))
               && ((val.stringArrayValue == null && this.stringArrayValue == null)
                   || Arrays.equals(val.stringArrayValue, this.stringArrayValue))

               && ((val.dateValue == null && this.dateValue == null)
                   || val.dateValue.equals(this.dateValue))
               && ((val.dateArrayValue == null && this.dateArrayValue == null)
                   || Arrays.equals(val.dateArrayValue, this.dateArrayValue))

               && ((val.mapValue == null && this.mapValue == null)
                   || val.mapValue.equals(this.mapValue))
               && ((val.mapArrayValue == null && this.mapArrayValue == null)
                   || Arrays.equals(val.mapArrayValue, this.mapArrayValue))

               && ((val.objectValue == null && this.objectValue == null)
                   || val.objectValue.equals(this.objectValue))
               && ((val.objectArrayValue == null && this.objectArrayValue == null)
                   || Arrays.equals(val.objectArrayValue, this.objectArrayValue))
                ;
    }
}