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
import org.apache.hadoop.hbase.hbql.client.Column;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.Schema;
import org.apache.hadoop.hbase.hbql.client.Util;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Random;

@Schema(name = "alltypes2")
public class AnnotatedAllTypes implements Serializable {

    @Column
    public String keyval = null;

    @Column
    public boolean booleanValue = true;

    @Column
    public boolean[] booleanArrayValue = null;

    @Column
    public byte byteValue = 0;

    @Column
    public byte[] byteArrayValue = null;

    @Column
    public char charValue = 0;

    @Column
    public char[] charArrayValue;

    @Column
    public short shortValue = 0;

    @Column
    public short[] shortArrayValue;

    @Column
    public int intValue = 0;

    @Column
    public int[] intArrayValue;

    @Column
    public long longValue = 0L;

    @Column
    public long[] longArrayValue = null;

    @Column
    public float floatValue = 0;

    @Column
    public float[] floatArrayValue = null;

    @Column
    public double doubleValue = 0;

    @Column
    public double[] doubleArrayValue = null;

    @Column
    public String stringValue = null;

    @Column
    public String[] stringArrayValue = null;

    @Column
    public Date dateValue = new Date();

    @Column
    public Date[] dateArrayValue = null;

    @Column
    public Map<String, String> mapValue = null;

    @Column
    public Map<String, String>[] mapArrayValue = null;

    @Column
    public TestObject objectValue = null;

    @Column
    public TestObject[] objectArrayValue = null;


    public static class TestObject implements Serializable {
        public int intvalue = random.nextInt();

        public boolean equals(final Object o) {

            if (!(o instanceof TestObject))
                return false;

            final TestObject val = (TestObject)o;

            return val.intvalue == this.intvalue;
        }
    }

    public AnnotatedAllTypes() {
    }

    static Random random = new Random();

    public AnnotatedAllTypes(final String keyval, final int intValue, final String stringValue) {
        this.keyval = keyval;
        this.intValue = intValue;
        this.stringValue = stringValue;
    }

    public void setSomeValues(int val, boolean noRandomData, int cnt) throws HBqlException {

        this.keyval = Util.getZeroPaddedNumber(val, 10);

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

        if (!(o instanceof AnnotatedAllTypes))
            return false;

        final AnnotatedAllTypes val = (AnnotatedAllTypes)o;

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
