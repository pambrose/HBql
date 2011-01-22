/*
 * Copyright (c) 2011.  The Apache Software Foundation
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

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.Util;
import org.apache.hadoop.hbase.hbql.util.Maps;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Random;

public class RecordAllTypes implements Serializable {

    public String    keyval            = null;
    public boolean   booleanValue      = true;
    public boolean[] booleanArrayValue = null;
    public byte      byteValue         = 0;
    public byte[]    byteArrayValue    = null;
    public char      charValue         = 0;
    public char[] charArrayValue;
    public short shortValue = 0;
    public short[] shortArrayValue;
    public int intValue = 0;
    public int[] intArrayValue;
    public long                  longValue        = 0L;
    public long[]                longArrayValue   = null;
    public float                 floatValue       = 0;
    public float[]               floatArrayValue  = null;
    public double                doubleValue      = 0;
    public double[]              doubleArrayValue = null;
    public String                stringValue      = null;
    public String[]              stringArrayValue = null;
    public Date                  dateValue        = new Date();
    public Date[]                dateArrayValue   = null;
    public Map<String, String>   mapValue         = null;
    public Map<String, String>[] mapArrayValue    = null;
    public TestObject            objectValue      = null;
    public TestObject[]          objectArrayValue = null;


    public static class TestObject implements Serializable {
        public int intvalue = random.nextInt();

        public boolean equals(final Object o) {

            if (!(o instanceof TestObject))
                return false;

            final TestObject val = (TestObject)o;

            return val.intvalue == this.intvalue;
        }
    }

    public RecordAllTypes() {
    }

    static Random random = new Random();

    public RecordAllTypes(final HRecord rec) throws HBqlException {
        this.keyval = (String)rec.getCurrentValue("keyval");
        this.booleanValue = (Boolean)rec.getCurrentValue("booleanValue");
        this.booleanArrayValue = (boolean[])rec.getCurrentValue("booleanArrayValue");
        this.byteValue = (Byte)rec.getCurrentValue("byteValue");
        this.byteArrayValue = (byte[])rec.getCurrentValue("byteArrayValue");
        this.charValue = (Character)rec.getCurrentValue("charValue");
        this.charArrayValue = (char[])rec.getCurrentValue("charArrayValue");
        this.shortValue = (Short)rec.getCurrentValue("shortValue");
        this.shortArrayValue = (short[])rec.getCurrentValue("shortArrayValue");
        this.intValue = (Integer)rec.getCurrentValue("intValue");
        this.intArrayValue = (int[])rec.getCurrentValue("intArrayValue");
        this.longValue = (Long)rec.getCurrentValue("longValue");
        this.longArrayValue = (long[])rec.getCurrentValue("longArrayValue");
        this.floatValue = (Float)rec.getCurrentValue("floatValue");
        this.floatArrayValue = (float[])rec.getCurrentValue("floatArrayValue");
        this.doubleValue = (Double)rec.getCurrentValue("doubleValue");
        this.doubleArrayValue = (double[])rec.getCurrentValue("doubleArrayValue");
        this.stringValue = (String)rec.getCurrentValue("stringValue");
        this.stringArrayValue = (String[])rec.getCurrentValue("stringArrayValue");
        this.dateValue = (Date)rec.getCurrentValue("dateValue");
        this.dateArrayValue = (Date[])rec.getCurrentValue("dateArrayValue");

        this.mapValue = (Map<String, String>)rec.getCurrentValue("mapValue");

        Object[] tmp = (Object[])rec.getCurrentValue("mapArrayValue");
        if (tmp != null) {
            this.mapArrayValue = new Map[tmp.length];
            int i = 0;
            for (Object obj : tmp)
                this.mapArrayValue[i++] = (Map<String, String>)obj;
        }
        else {
            this.mapArrayValue = null;
        }

        this.objectValue = (TestObject)rec.getCurrentValue("objectValue");

        tmp = (Object[])rec.getCurrentValue("objectArrayValue");
        if (tmp != null) {
            this.objectArrayValue = new TestObject[tmp.length];
            int i = 0;
            for (Object obj : tmp)
                this.objectArrayValue[i++] = (TestObject)obj;
        }
        else {
            this.objectArrayValue = null;
        }
    }

    public HRecord getHRecord(final HConnection connection) throws HBqlException {

        HRecord rec = connection.getMapping("alltypes").newHRecord();

        rec.setCurrentValue("keyval", this.keyval);
        rec.setCurrentValue("booleanValue", this.booleanValue);
        rec.setCurrentValue("booleanArrayValue", this.booleanArrayValue);
        rec.setCurrentValue("byteValue", this.byteValue);
        rec.setCurrentValue("byteArrayValue", this.byteArrayValue);
        rec.setCurrentValue("charValue", this.charValue);
        rec.setCurrentValue("charArrayValue", this.charArrayValue);
        rec.setCurrentValue("shortValue", this.shortValue);
        rec.setCurrentValue("shortArrayValue", this.shortArrayValue);
        rec.setCurrentValue("intValue", this.intValue);
        rec.setCurrentValue("intArrayValue", this.intArrayValue);
        rec.setCurrentValue("longValue", this.longValue);
        rec.setCurrentValue("longArrayValue", this.longArrayValue);
        rec.setCurrentValue("floatValue", this.floatValue);
        rec.setCurrentValue("floatArrayValue", this.floatArrayValue);
        rec.setCurrentValue("doubleValue", this.doubleValue);
        rec.setCurrentValue("doubleArrayValue", this.doubleArrayValue);
        rec.setCurrentValue("stringValue", this.stringValue);
        rec.setCurrentValue("stringArrayValue", this.stringArrayValue);
        rec.setCurrentValue("dateValue", this.dateValue);
        rec.setCurrentValue("dateArrayValue", this.dateArrayValue);
        rec.setCurrentValue("mapValue", this.mapValue);
        rec.setCurrentValue("mapArrayValue", this.mapArrayValue);
        rec.setCurrentValue("objectValue", this.objectValue);
        rec.setCurrentValue("objectArrayValue", this.objectArrayValue);

        return rec;
    }


    public void setSomeValues(int val, boolean noRandomData, int cnt) throws HBqlException {

        this.keyval = Util.getZeroPaddedNonNegativeNumber(val, 10);

        if (noRandomData || random.nextBoolean()) {
            this.booleanValue = random.nextBoolean();
            this.booleanArrayValue = new boolean[cnt];
            for (int i = 0; i < cnt; i++)
                this.booleanArrayValue[i] = random.nextBoolean();
        }

        if (noRandomData || random.nextBoolean()) {
            this.byteValue = (byte)random.nextInt();
            this.byteArrayValue = new byte[cnt];
            for (int i = 0; i < cnt; i++)
                this.byteArrayValue[i] = (byte)random.nextInt();
        }

        if (noRandomData || random.nextBoolean()) {
            this.charValue = ("" + random.nextInt()).charAt(0);
            this.charArrayValue = ("" + random.nextInt()).toCharArray();
        }

        if (noRandomData || random.nextBoolean()) {
            this.shortValue = (short)random.nextInt();
            this.shortArrayValue = new short[cnt];
            for (int i = 0; i < cnt; i++)
                this.shortArrayValue[i] = (short)random.nextInt();
        }

        if (noRandomData || random.nextBoolean()) {
            this.intValue = random.nextInt();
            this.intArrayValue = new int[cnt];
            for (int i = 0; i < cnt; i++)
                this.intArrayValue[i] = random.nextInt();
        }

        if (noRandomData || random.nextBoolean()) {
            this.longValue = random.nextLong();
            this.longArrayValue = new long[cnt];
            for (int i = 0; i < cnt; i++)
                this.longArrayValue[i] = random.nextLong();
        }

        if (noRandomData || random.nextBoolean()) {
            this.floatValue = random.nextFloat();
            this.floatArrayValue = new float[cnt];
            for (int i = 0; i < cnt; i++)
                this.floatArrayValue[i] = random.nextFloat();
        }

        if (noRandomData || random.nextBoolean()) {
            this.doubleValue = random.nextDouble();
            this.doubleArrayValue = new double[cnt];
            for (int i = 0; i < cnt; i++)
                this.doubleArrayValue[i] = random.nextDouble();
        }

        if (noRandomData || random.nextBoolean()) {
            this.stringValue = "" + random.nextDouble();
            this.stringArrayValue = new String[cnt];
            for (int i = 0; i < cnt; i++)
                this.stringArrayValue[i] = "" + random.nextDouble();
        }

        if (noRandomData || random.nextBoolean()) {
            this.dateValue = new Date(random.nextLong());
            this.dateArrayValue = new Date[cnt];
            for (int i = 0; i < cnt; i++)
                this.dateArrayValue[i] = new Date(random.nextLong());
        }

        if (noRandomData || random.nextBoolean()) {
            this.mapValue = getRandomMap(cnt);

            this.mapArrayValue = new Map[cnt];
            for (int i = 0; i < cnt; i++)
                this.mapArrayValue[i] = getRandomMap(cnt);
        }

        if (noRandomData || random.nextBoolean()) {
            this.objectValue = new TestObject();
            this.objectArrayValue = new TestObject[cnt];
            for (int i = 0; i < cnt; i++)
                this.objectArrayValue[i] = new TestObject();
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