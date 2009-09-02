import com.google.common.collect.Lists;
import com.imap4j.hbase.hbql.HColumn;
import com.imap4j.hbase.hbql.HFamily;
import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.HPersistable;
import com.imap4j.hbase.hbql.HTable;
import com.imap4j.hbase.hbql.io.Serialization;
import com.imap4j.hbase.hbql.schema.FieldType;
import com.imap4j.hbase.hbql.test.HTest;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 27, 2009
 * Time: 2:13:47 PM
 */
public class WhereExpressionTests extends HTest {

    @HTable(name = "alltypes",
            families = {
                    @HFamily(name = "family1", maxVersions = 10),
                    @HFamily(name = "family2"),
                    @HFamily(name = "family3", maxVersions = 5)
            })
    public static class AllTypes implements HPersistable {

        @HColumn(key = true)
        private String keyval = null;

        @HColumn(family = "family1")
        private int intValue = -1;

        @HColumn(family = "family1")
        private String stringValue = "";

        public AllTypes() {
        }

        public AllTypes(final String keyval, final int intValue, final String stringValue) {
            this.keyval = keyval;
            this.intValue = intValue;
            this.stringValue = stringValue;
        }
    }

    @Test
    public void booleanExpressions() throws HPersistException {
        assertEvalTrue("TRUE");
        assertEvalFalse("NOT TRUE");
        assertEvalFalse("! TRUE");
        assertEvalFalse("!TRUE");
        assertEvalFalse("!(((((TRUE)))))");
        assertEvalTrue("((TRUE))");
        assertEvalTrue("(((((TRUE)))))");
        assertEvalFalse("!((!(((!TRUE)))))");
        assertEvalFalse("FALSE");
        assertEvalTrue("TRUE OR TRUE");
        assertEvalTrue("TRUE OR TRUE OR TRUE");
        assertEvalFalse("FALSE OR FALSE OR FALSE");
        assertEvalFalse("(FALSE OR FALSE OR FALSE)");
        assertEvalFalse("((((FALSE OR FALSE OR FALSE))))" + " OR " + "((((FALSE OR FALSE OR FALSE))))");
        assertEvalTrue("TRUE OR FALSE");
        assertEvalFalse("FALSE OR FALSE");
        assertEvalTrue("TRUE AND TRUE");
        assertEvalFalse("TRUE AND FALSE");
        assertEvalTrue("TRUE OR ((true) or true) OR FALSE");
        assertEvalFalse("(false AND ((true) OR true)) AND TRUE");
        assertEvalTrue("(false AND ((true) OR true)) OR TRUE");
    }

    @Test
    public void numericCompares() throws HPersistException {

        assertEvalTrue("4 < 5");
        assertEvalFalse("4 = 5");
        assertEvalFalse("4 == 5");
        assertEvalTrue("4 != 5");
        assertEvalTrue("4 <> 5");
        assertEvalTrue("4 <= 5");
        assertEvalFalse("4 > 5");
        assertEvalFalse("4 >= 5");
    }

    @Test
    public void stringCompares() throws HPersistException {

        assertEvalTrue("'aaa' == 'aaa'");
        assertEvalFalse("'aaa' != 'aaa'");
        assertEvalFalse("'aaa' <> 'aaa'");
        assertEvalFalse("'aaa' == 'bbb'");
        assertEvalTrue("'aaa' <= 'bbb'");
        assertEvalTrue("'bbb' <= 'bbb'");
        assertEvalFalse("'bbb' <= 'aaa'");
        assertEvalFalse("'bbb' > 'bbb'");
        assertEvalTrue("'bbb' > 'aaa'");
        assertEvalTrue("'bbb' >= 'aaa'");
        assertEvalTrue("'aaa' >= 'aaa'");
    }

    @Test
    public void nullCompares() throws HPersistException {

        assertEvalTrue("NULL IS NULL");
        assertEvalFalse("NULL IS NOT NULL");
    }

    @Test
    public void numericCalculations() throws HPersistException {

        assertEvalTrue("9 == 9");
        assertEvalTrue("((4 + 5) == 9)");
        assertEvalTrue("(9) == 9");
        assertEvalTrue("(4 + 5) == 9");
        assertEvalFalse("(4 + 5) == 8");
        assertEvalTrue("(4 + 5 + 10 + 10 - 20) == 9");
        assertEvalFalse("(4 + 5 + 10 + 10 - 20) != 9");

        assertEvalTrue("(4 * 5) == 20");
        assertEvalTrue("(40 % 6) == 4");
        assertEvalFalse("(40 % 6) == 3");
    }

    @Test
    public void numericFunctions() throws HPersistException {

        assertEvalTrue("3 between 2 AND 5");
        assertEvalTrue("3 between (1+1) AND (3+2)");
        assertEvalTrue("3 between (1+1) && (3+2)");

        assertEvalTrue("3 in (2,3,4)");
        assertEvalFalse("3 in (1+1,1+3,4)");
        assertEvalTrue("3 in (1+1,1+2,4)");
        assertEvalFalse("3 !in (1+1,1+2,4)");
        assertEvalFalse("3 NOT in (1+1,1+2,4)");
        assertEvalTrue("3 == [true ? 3 : 2]");
        assertEvalFalse("3 == [false ? 3 : 2]");
        assertEvalTrue("2 == [false ? 3 : 2]");

    }

    @Test
    public void stringFunctions() throws HPersistException {

        assertEvalTrue("'bbb' between 'aaa' AND 'ccc'");
        assertEvalTrue("'bbb' between 'aaa' && 'ccc'");
        assertEvalTrue("'bbb' between 'bbb' AND 'ccc'");
        assertEvalFalse("'bbb' between 'ccc' AND 'ddd'");
        assertEvalTrue("('bbb' between 'bbb' AND 'ccc') AND ('fff' between 'eee' AND 'ggg')");
        assertEvalTrue("('bbb' between 'bbb' && 'ccc') || ('fff' between 'eee' && 'ggg')");
        assertEvalFalse("('bbb' not between 'bbb' AND 'ccc') AND ('fff' between 'eee' AND 'ggg')");
        assertEvalTrue("'bbb' == LOWER('BBB')");
        assertEvalTrue("'ABABAB' == UPPER(CONCAT('aba', 'bab'))");
        assertEvalTrue("'bbb' == SUBSTRING('BBBbbbAAA', 3, 6)");
        assertEvalTrue("'AAA' == 'A' + 'A' + 'A'");
        assertEvalTrue("'aaa' == LOWER('A' + 'A' + 'A')");
    }

    @Test
    public void objectFunctions() throws HPersistException {

        final AllTypes obj = new AllTypes("aaa", 3, "bbb");

        assertEvalTrue(obj, "stringValue between 'aaa' AND 'ccc'");
        assertEvalTrue(obj, "stringValue between 'aaa' AND 'ccc' AND stringValue between 'aaa' AND 'ccc'");
        assertEvalTrue(obj, "stringValue between 'bbb' AND 'ccc'");
        assertEvalFalse(obj, "stringValue between 'ccc' AND 'ddd'");
        assertEvalTrue(obj, "('bbb' between stringValue AND 'ccc') AND ('fff' between 'eee' AND 'ggg')");

        assertEvalTrue(obj, "intValue between 2 AND 5");
        assertEvalTrue(obj, "intValue between (1+1) AND (intValue+2)");
        assertEvalFalse(obj, "stringValue IN ('v2', 'v0', 'v999')");
        assertEvalTrue(obj, "'v19' = 'v19'");
        assertEvalFalse(obj, "'v19'= stringValue");
        assertEvalFalse(obj, "stringValue = 'v19'");
        assertEvalFalse(obj, "stringValue = 'v19' OR stringValue IN ('v2', 'v0', 'v999')");
        assertEvalTrue(obj, "stringValue IS NOT NULL");
        assertEvalFalse(obj, "stringValue IS NULL");
    }

    @Test
    public void columnLookups() throws HPersistException {
        assertColumnsMatchTrue("TRUE");
        assertColumnsMatchFalse("TRUE", "intValue");
        assertColumnsMatchTrue("intValue between 2 AND 5", "intValue");
        assertColumnsMatchFalse("xintValue between 2 AND 5", "intValue");
        assertColumnsMatchTrue("a1 < a2", "a1", "a2");
        assertColumnsMatchFalse("a1 < a2 || d1 > k3", "a1", "a2");
        assertColumnsMatchTrue("a1 < a2 || d1 > k3", "a1", "a2", "d1", "k3");
    }

    @Test
    public void hadoopSerialization() throws IOException, HPersistException {

        final int total = 100000;
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
            final StringBuffer sbuf = new StringBuffer();
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
        }
    }

}

