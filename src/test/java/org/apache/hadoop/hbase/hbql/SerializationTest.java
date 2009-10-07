package org.apache.hadoop.hbase.hbql;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.io.Serialization;
import org.apache.hadoop.hbase.hbql.query.schema.FieldType;
import org.apache.hadoop.hbase.hbql.query.util.Lists;
import org.apache.hadoop.hbase.hbql.util.TestSupport;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class SerializationTest extends TestSupport {

    public static class TestClass implements Serializable {
        String strval;
        int intval;
        double doubleval;

        public TestClass(final String strval, final int intval, final double doubleval) {
            this.strval = strval;
            this.intval = intval;
            this.doubleval = doubleval;
        }

        public boolean equals(final Object o) {
            TestClass t = (TestClass)o;
            return this.strval.equals(t.strval)
                   && this.intval == t.intval
                   && this.doubleval == t.doubleval;
        }
    }

    @Test
    public void hadoopSerialization() throws HBqlException {

        final int total = 1000;
        int pos = 0;
        byte[] b;

        final Serialization.TYPE[] types = {Serialization.TYPE.HADOOP, Serialization.TYPE.JAVA};

        for (final Serialization.TYPE type : types) {

            Serialization ser = Serialization.getSerializationStrategy(type);

            final Random random = new Random(System.currentTimeMillis());

            // Boolean array
            final List<Boolean> boolList = Lists.newArrayList();
            for (int i = 0; i < total; i++)
                boolList.add((random.nextInt() % 2) > 0);
            final boolean[] boolarr1 = new boolean[boolList.size()];
            pos = 0;
            for (final Boolean val : boolList)
                boolarr1[pos++] = val;
            b = ser.getArrayasBytes(FieldType.BooleanType, boolarr1);
            final boolean[] boolarr2 = (boolean[])ser.getArrayFromBytes(FieldType.BooleanType, Boolean.TYPE, b);
            assertTrue(Arrays.equals(boolarr1, boolarr2));

            // Byte array
            final List<Byte> byteList = Lists.newArrayList();
            for (int i = 0; i < total; i++)
                byteList.add((byte)(random.nextInt() % Byte.MAX_VALUE));
            final byte[] bytearr1 = new byte[byteList.size()];
            pos = 0;
            for (final Byte val : byteList)
                bytearr1[pos++] = val;
            b = ser.getArrayasBytes(FieldType.ByteType, bytearr1);
            final byte[] bytearr2 = (byte[])ser.getArrayFromBytes(FieldType.ByteType, Byte.TYPE, b);
            assertTrue(Arrays.equals(bytearr1, bytearr2));

            // Char array
            final StringBuilder sbuf = new StringBuilder();
            for (int i = 0; i < total; i++) {
                String s = "" + System.nanoTime();
                String t = s.substring(s.length() - 5, s.length() - 4);
                sbuf.append(t);
            }
            final char[] chararr1 = sbuf.toString().toCharArray();
            b = ser.getArrayasBytes(FieldType.CharType, chararr1);
            final char[] chararr2 = (char[])ser.getArrayFromBytes(FieldType.CharType, Short.TYPE, b);
            assertTrue(Arrays.equals(chararr1, chararr2));

            // Short array
            final List<Short> shortList = Lists.newArrayList();
            for (int i = 0; i < total; i++)
                shortList.add((short)random.nextInt());
            final short[] shortarr1 = new short[shortList.size()];
            pos = 0;
            for (final Short val : shortList)
                shortarr1[pos++] = val;
            b = ser.getArrayasBytes(FieldType.ShortType, shortarr1);
            final short[] shortarr2 = (short[])ser.getArrayFromBytes(FieldType.ShortType, Short.TYPE, b);
            assertTrue(Arrays.equals(shortarr1, shortarr2));

            // Int array
            final List<Integer> intList = Lists.newArrayList();
            for (int i = 0; i < total; i++)
                intList.add(random.nextInt());
            final int[] intarr1 = new int[intList.size()];
            pos = 0;
            for (final Integer val : intList)
                intarr1[pos++] = val;
            b = ser.getArrayasBytes(FieldType.IntegerType, intarr1);
            final int[] intarr2 = (int[])ser.getArrayFromBytes(FieldType.IntegerType, Integer.TYPE, b);
            assertTrue(Arrays.equals(intarr1, intarr2));

            // Long Array
            final List<Long> longList = Lists.newArrayList();
            for (int i = 0; i < total; i++)
                longList.add(random.nextLong());
            final long[] longarr1 = new long[longList.size()];
            pos = 0;
            for (final Long val : longList)
                longarr1[pos++] = val;
            b = ser.getArrayasBytes(FieldType.LongType, longarr1);
            final long[] longarr2 = (long[])ser.getArrayFromBytes(FieldType.LongType, Long.TYPE, b);
            assertTrue(Arrays.equals(longarr1, longarr2));

            // Float Array
            final List<Float> floatList = Lists.newArrayList();
            for (int i = 0; i < total; i++)
                floatList.add(random.nextFloat());
            final float[] floatarr1 = new float[floatList.size()];
            pos = 0;
            for (final Float val : floatList)
                floatarr1[pos++] = val;
            b = ser.getArrayasBytes(FieldType.FloatType, floatarr1);
            final float[] floatarr2 = (float[])ser.getArrayFromBytes(FieldType.FloatType, Float.TYPE, b);
            assertTrue(Arrays.equals(floatarr1, floatarr2));

            // Double Array
            final List<Double> doubleList = Lists.newArrayList();
            for (int i = 0; i < total; i++)
                doubleList.add(random.nextDouble());
            final double[] doublearr1 = new double[doubleList.size()];
            pos = 0;
            for (final Double val : doubleList)
                doublearr1[pos++] = val;
            b = ser.getArrayasBytes(FieldType.DoubleType, doublearr1);
            final double[] doublearr2 = (double[])ser.getArrayFromBytes(FieldType.DoubleType, Double.TYPE, b);
            assertTrue(Arrays.equals(doublearr1, doublearr2));

            // String Array
            final List<String> stringList = Lists.newArrayList();
            for (int i = 0; i < total; i++)
                stringList.add((new Date(random.nextLong()).toString()));
            final String[] stringarr1 = new String[stringList.size()];
            pos = 0;
            for (final String val : stringList)
                stringarr1[pos++] = val;
            b = ser.getArrayasBytes(FieldType.StringType, stringarr1);
            final String[] stringarr2 = (String[])ser.getArrayFromBytes(FieldType.StringType, String.class, b);
            assertTrue(Arrays.equals(stringarr1, stringarr2));

            // Date Array
            final List<Date> dateList = Lists.newArrayList();
            for (int i = 0; i < total; i++)
                dateList.add(new Date(random.nextLong()));
            final Date[] datearr1 = new Date[dateList.size()];
            pos = 0;
            for (final Date val : dateList)
                datearr1[pos++] = val;
            b = ser.getArrayasBytes(FieldType.DateType, datearr1);
            final Date[] datearr2 = (Date[])ser.getArrayFromBytes(FieldType.DateType, Date.class, b);
            assertTrue(Arrays.equals(datearr1, datearr2));

            // Object Array
            final List<Object> objectList = Lists.newArrayList();
            for (int i = 0; i < total; i++)
                objectList.add(new TestClass("" + random.nextDouble(), random.nextInt(), random.nextDouble()));
            final Object[] objarr1 = new Object[objectList.size()];
            pos = 0;
            for (final Object val : objectList)
                objarr1[pos++] = val;
            b = ser.getArrayasBytes(FieldType.ObjectType, objarr1);
            final Object[] objarr2 = (Object[])ser.getArrayFromBytes(FieldType.ObjectType, Object.class, b);
            assertTrue(Arrays.equals(objarr1, objarr2));
        }
    }
}
