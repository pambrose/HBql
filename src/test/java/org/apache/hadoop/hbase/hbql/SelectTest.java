package org.apache.hadoop.hbase.hbql;

import org.apache.hadoop.hbase.hbql.client.HBatch;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HConnectionManager;
import org.apache.hadoop.hbase.hbql.client.HQuery;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HResults;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.util.HUtil;
import org.apache.hadoop.hbase.hbql.query.util.Lists;
import org.apache.hadoop.hbase.hbql.query.util.Maps;
import org.apache.hadoop.hbase.hbql.util.TestSupport;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;

public class SelectTest extends TestSupport {

    static HConnection conn = null;
    static List<String> keyList = Lists.newArrayList();
    static List<String> val1List = Lists.newArrayList();
    static List<Integer> val5List = Lists.newArrayList();
    static int[] val8check = null;

    static Random randomVal = new Random();

    @BeforeClass
    public static void onetimeSetup() throws HBqlException, IOException {

        SchemaManager.execute("define schema table1 alias tab1"
                              + "("
                              + "keyval key, "
                              + "f1:val1 string alias val1, "
                              + "f1:val2 string alias val2, "
                              + "f1:val3 string alias notdefinedval, "
                              + "f2:val1 date alias val3, "
                              + "f2:val2 date alias val4, "
                              + "f3:val1 int alias val5, "
                              + "f3:val2 int alias val6, "
                              + "f3:val3 int alias val7, "
                              + "f3:val4 int[] alias val8, "
                              + "f3:mapval1 string mapKeysAsColumns alias f3mapval1, "
                              + "f3:mapval2 string mapKeysAsColumns alias f3mapval2 "
                              + ")");

        conn = HConnectionManager.newHConnection();

        if (!conn.tableExists("table1"))
            System.out.println(conn.execute("create table using table1"));
        else {
            System.out.println(conn.execute("delete from table1"));
            //System.out.println(conn.execute("disable table table1"));
            //System.out.println(conn.execute("drop table table1"));
        }

        insertRecords(conn, 10, "Batch 1");
        insertRecords(conn, 10, "Batch 2");

        keyList.clear();
        val1List.clear();
        val5List.clear();
        val8check = null;

        insertRecords(conn, 10, "Batch 3");
    }

    private static void insertRecords(final HConnection conn,
                                      final int cnt,
                                      final String msg) throws HBqlException, IOException {

        final HBatch batch = new HBatch();
        for (int i = 0; i < cnt; i++) {

            final HRecord rec = SchemaManager.newHRecord("table1");

            final String keyval = HUtil.getZeroPaddedNumber(i, 10);
            keyList.add(keyval);
            rec.setCurrentValue("keyval", keyval);

            int val5 = randomVal.nextInt();
            String s_val5 = "" + val5;
            val1List.add(s_val5);
            val5List.add(val5);

            rec.setCurrentValue("val1", s_val5);
            rec.setCurrentValue("val2", s_val5 + " " + msg);
            rec.setCurrentValue("val5", val5);
            rec.setCurrentValue("val6", i * 100);

            Map<String, String> mapval1 = Maps.newHashMap();
            mapval1.put("mapcol1", "mapcol1 val" + i + " " + msg);
            mapval1.put("mapcol2", "mapcol2 val" + i + " " + msg);

            rec.setCurrentValue("f3mapval1", mapval1);

            Map<String, String> mapval2 = Maps.newHashMap();
            mapval2.put("mapcol1-b", "mapcol1-b val" + i + " " + msg);
            mapval2.put("mapcol2-b", "mapcol2-b val" + i + " " + msg);
            mapval2.put("mapcol3-b", "mapcol3-b val" + i + " " + msg);

            rec.setCurrentValue("f3mapval2", mapval2);

            int[] intv1 = new int[5];
            val8check = new int[5];
            for (int j = 0; j < intv1.length; j++) {
                intv1[j] = j * 10;
                val8check[j] = intv1[j];
            }

            rec.setCurrentValue("val8", intv1);

            batch.insert(rec);
        }

        conn.apply(batch);
    }


    @Test
    public void selectExpressions() throws HBqlException, IOException {

        final String query1 = "SELECT val1, val5, (val5 - val5 + val5) as val6, (val5+val5) as val7 FROM table1";

        HQuery<HRecord> q1 = conn.newHQuery(query1);

        HResults<HRecord> results1 = q1.getResults();

        List<String> testKeyVals = Lists.newArrayList();
        List<String> testVal1Vals = Lists.newArrayList();
        List<Integer> testVal5Vals = Lists.newArrayList();
        List<Integer> testVal6Vals = Lists.newArrayList();

        int rec_cnt = 0;
        for (HRecord rec : results1) {

            String keyval = (String)rec.getCurrentValue("keyval");
            String val1 = (String)rec.getCurrentValue("val1");
            int val5 = (Integer)rec.getCurrentValue("val5");
            int val6 = (Integer)rec.getCurrentValue("val6");

            testKeyVals.add(keyval);
            testVal1Vals.add(val1);
            testVal5Vals.add(val5);
            testVal6Vals.add(val6);

            System.out.println("Current Values: " + keyval
                               + " - " + rec.getCurrentValue("val1")
                               + " - " + rec.getCurrentValue("val5")
                               + " - " + rec.getCurrentValue("val6")
            );
            rec_cnt++;
        }

        assertTrue(testKeyVals.equals(keyList));
        assertTrue(testVal1Vals.equals(val1List));
        assertTrue(testVal5Vals.equals(val5List));
        assertTrue(testVal6Vals.equals(val5List));

        HQuery<HRecord> q2 = conn.newHQuery(query1);
        List<HRecord> recList2 = q1.getResultList();
        assertTrue(recList2.size() == rec_cnt);

        final String query3 = "SELECT val1, val5, (val5 - val5 + val5) as val6, (val5+val5) as val7 FROM table1 " +
                              "WITH KEYS '0000000001' , '0000000002'";
        HQuery<HRecord> q3 = conn.newHQuery(query3);
        List<HRecord> recList3 = q3.getResultList();
        assertTrue(recList3.size() == 2);

        final String query4 = "SELECT val1, val5, (val5 - val5 + val5) as val6, (val5+val5) as val7 FROM table1 " +
                              "WITH KEYS :key1";
        HQuery<HRecord> q4 = conn.newHQuery(query4);
        q4.setParameter("key1", "0000000001");
        List<HRecord> recList4 = q4.getResultList();
        assertTrue(recList4.size() == 1);

        final String query5 = "SELECT val1, val5, (val5 - val5 + val5) as val6, (val5+val5) as val7 FROM table1 " +
                              "WITH KEYS :key1, :key2";
        HQuery<HRecord> q5 = conn.newHQuery(query5);
        q5.setParameter("key1", "0000000001");
        q5.setParameter("key2", "0000000002");
        List<HRecord> recList5 = q5.getResultList();
        assertTrue(recList5.size() == 2);

        final String query6 = "SELECT val1, val5, (val5 - val5 + val5) as val6, (val5+val5) as val7 FROM table1 " +
                              "WITH KEYS :key1";
        HQuery<HRecord> q6 = conn.newHQuery(query6);
        List<String> listOfKeys = Lists.newArrayList();
        listOfKeys.add("0000000001");
        listOfKeys.add("0000000002");
        listOfKeys.add("0000000003");
        q6.setParameter("key1", listOfKeys);
        List<HRecord> recList6 = q6.getResultList();
        assertTrue(recList6.size() == 3);
    }

    @Test
    public void selectMapExpressions() throws HBqlException, IOException {

        final String query1 = "SELECT f3mapval1 FROM table1";
        HQuery<HRecord> q1 = conn.newHQuery(query1);
        List<HRecord> recList1 = q1.getResultList();
        assertTrue(recList1.size() == 10);

        final String query2 = "SELECT f3mapval1, f3mapval2 FROM table1";
        HQuery<HRecord> q2 = conn.newHQuery(query2);
        List<HRecord> recList2 = q2.getResultList();
        assertTrue(recList2.size() == 10);

        for (final HRecord rec : recList2) {
            Map<String, Object> map1 = rec.getKeysAsColumnsMap("f3mapval1");
            Map<String, Object> map2 = rec.getKeysAsColumnsMap("f3mapval2");

            assertTrue(map1.size() == 2);
            assertTrue(map2.size() == 3);

            Map<String, NavigableMap<Long, Object>> vmap1 = rec.getKeysAsColumnsVersionMap("f3mapval1");
            Map<String, NavigableMap<Long, Object>> vmap2 = rec.getKeysAsColumnsVersionMap("f3mapval2");

            assertTrue(vmap1.size() == 2);
            assertTrue(vmap2.size() == 3);
        }
    }

    @Test
    public void selectVectorExpressions() throws HBqlException, IOException {

        final String query1 = "SELECT val8 FROM table1";
        HQuery<HRecord> q1 = conn.newHQuery(query1);
        List<HRecord> recList1 = q1.getResultList();
        assertTrue(recList1.size() == 10);

        for (final HRecord rec : recList1) {
            int[] intv = (int[])rec.getCurrentValue("val8");
            assertTrue(intv.length == 5);
        }
    }

    @Test
    public void selectInvalidColumnReferences() throws HBqlException, IOException {

        final String query1 = "SELECT * FROM table1 with client FILTER where notdefinedval = 'dd'";
        HQuery<HRecord> q1 = conn.newHQuery(query1);
        List<HRecord> recList1 = q1.getResultList();
        assertTrue(recList1.size() == 0);

        final String query2 = "SELECT * FROM table1 with client FILTER where DEFINEDINROW(notdefinedval)";
        HQuery<HRecord> q2 = conn.newHQuery(query2);
        List<HRecord> recList2 = q2.getResultList();
        assertTrue(recList2.size() == 0);

        final String query3 = "SELECT * FROM table1 with client FILTER where NOT DEFINEDINROW(notdefinedval)";
        HQuery<HRecord> q3 = conn.newHQuery(query3);
        List<HRecord> recList3 = q3.getResultList();
        assertTrue(recList3.size() == 10);

        final String query4 = "SELECT * FROM table1 with client FILTER where DEFINEDINROW(f1:val1)";
        HQuery<HRecord> q4 = conn.newHQuery(query4);
        List<HRecord> recList4 = q4.getResultList();
        assertTrue(recList4.size() == 10);

        final String query5 = "SELECT * FROM table1 with client FILTER where NOT DEFINEDINROW(f1:val1)";
        HQuery<HRecord> q5 = conn.newHQuery(query5);
        List<HRecord> recList5 = q5.getResultList();
        assertTrue(recList5.size() == 0);
    }

    @Test
    public void selectVectorVersionExpressions() throws HBqlException, IOException {

        final String query1 = "SELECT f1:val2, val8 FROM table1 WITH VERSIONS 5";
        HQuery<HRecord> q1 = conn.newHQuery(query1);
        List<HRecord> recList1 = q1.getResultList();
        assertTrue(recList1.size() == 10);

        for (final HRecord rec : recList1) {
            Map<Long, Object> m1 = rec.getVersionMap("f1:val2");
            assertTrue(m1.size() == 3);

            Map<Long, Object> m2 = rec.getVersionMap("val8");
            assertTrue(m2.size() == 3);

            for (Object obj : m2.values()) {
                int[] val8 = (int[])obj;
                for (int i = 0; i < val8.length; i++)
                    assertTrue(val8[i] == val8check[i]);
            }
        }
    }

    @Test
    public void selectFamiliesExpressions() throws HBqlException, IOException {

        SchemaManager.execute("drop schema table1");

        SchemaManager.execute("define schema table1 alias tab1"
                              + "("
                              + "keyval key, "
                              //  + "f1:val1 string alias val1, "
                              + "f1:val2 string alias val2, "
                              + "f1:* alias f1default, "
                              + "f2:val1 date alias val3, "
                              + "f2:val2 date alias val4, "
                              + "f3:val1 int alias val5, "
                              + "f3:val2 int alias val6, "
                              + "f3:val3 int alias val7, "
                              + "f3:val4 int[] alias val8, "
                              + "f3:mapval1 string mapKeysAsColumns alias f3mapval1, "
                              + "f3:mapval2 string mapKeysAsColumns alias f3mapval2 "
                              + ")");

        HQuery<HRecord> q1 = conn.newHQuery("SELECT f1:* FROM table1");
        List<HRecord> recList1 = q1.getResultList();
        assertTrue(recList1.size() == 10);

        HQuery<HRecord> q2 = conn.newHQuery("SELECT f1:* FROM table1 WITH VERSIONS 5");
        List<HRecord> recList2 = q2.getResultList();
        assertTrue(recList2.size() == 10);

        HQuery<HRecord> q3 = conn.newHQuery("SELECT * FROM table1");
        List<HRecord> recList3 = q3.getResultList();
        assertTrue(recList3.size() == 10);

        HQuery<HRecord> q4 = conn.newHQuery("SELECT * FROM table1 WITH VERSIONS 5");
        List<HRecord> recList4 = q4.getResultList();
        assertTrue(recList4.size() == 10);

        for (final HRecord rec : recList4) {
            Map<Long, Object> m1 = rec.getVersionMap("val2");
            assertTrue(m1.size() == 3);

            Map<Long, Object> m2 = rec.getVersionMap("val8");
            assertTrue(m2.size() == 3);

            for (Object obj : m2.values()) {
                int[] val8 = (int[])obj;
                for (int i = 0; i < val8.length; i++)
                    assertTrue(val8[i] == val8check[i]);
            }
        }
    }

    @Test
    public void selectUndefinedExpressions() throws HBqlException, IOException {

        SchemaManager.execute("drop schema table1");

        SchemaManager.execute("define schema table1 alias tab1"
                              + "("
                              + "keyval key, "
                              + "f1:* alias f1default "
                              + ")");

        final String query1 = "SELECT f1:val1, f1:val2 FROM table1";
        HQuery<HRecord> q1 = conn.newHQuery(query1);
        List<HRecord> recList1 = q1.getResultList();
        assertTrue(recList1.size() == 10);

        int i = 0;
        for (final HRecord rec : recList1) {
            Map<String, byte[]> vals = rec.getFamilyDefaultValueMap("f1default");
            assertTrue(vals.size() == 2);
            String val1 = HUtil.getSerialization().getStringFromBytes(vals.get("f1:val1"));
            assertTrue(val1List.get(i).equals(val1));
            i++;
        }
    }

    @Test
    public void selectUndefinedVersionExpressions() throws HBqlException, IOException {

        SchemaManager.execute("drop schema table1");

        SchemaManager.execute("define schema table1 alias tab1"
                              + "("
                              + "keyval key, "
                              + "f1:* alias f1default "
                              + ")");

        final String query1 = "SELECT f1:val1, f1:val2 FROM table1 WITH VERSIONS 5";
        HQuery<HRecord> q1 = conn.newHQuery(query1);
        List<HRecord> recList1 = q1.getResultList();
        assertTrue(recList1.size() == 10);

        int i = 0;
        for (final HRecord rec : recList1) {
            Map<String, byte[]> vals = rec.getFamilyDefaultValueMap("f1default");
            assertTrue(vals.size() == 2);
            String val1 = HUtil.getSerialization().getStringFromBytes(vals.get("f1:val1"));
            assertTrue(val1List.get(i).equals(val1));

            Map<String, NavigableMap<Long, byte[]>> vers = rec.getFamilyDefaultVersionMap("f1default");
            assertTrue(vers.size() == 2);
            NavigableMap<Long, byte[]> v2 = vers.get("f1:val1");
            assertTrue(v2.size() == 3);
            i++;
        }
    }

    @Test
    public void selectUnknownExpressions() throws HBqlException, IOException {

        SchemaManager.execute("drop schema table1");

        SchemaManager.execute("define schema table1 alias tab1"
                              + "("
                              + "keyval key, "
                              + "f1:* alias f1default "
                              + ")");

        final String query1 = "SELECT f1:valunknown FROM table1";
        HQuery<HRecord> q1 = conn.newHQuery(query1);
        List<HRecord> recList1 = q1.getResultList();
        assertTrue(recList1.size() == 10);

        int i = 0;
        for (final HRecord rec : recList1) {
            Map<String, byte[]> vals = rec.getFamilyDefaultValueMap("f1default");
            assertTrue(vals.size() == 1);
            String val1 = HUtil.getSerialization().getStringFromBytes(vals.get("f1:valunknown"));
            assertTrue(val1 == null);
            i++;
        }
    }

    @Test
    public void selectUnknownCalcExpressions() throws HBqlException, IOException {

        SchemaManager.execute("drop schema table1");

        SchemaManager.execute("define schema table1 alias tab1"
                              + "("
                              + "keyval key, "
                              + "f1:* alias f1default "
                              + ")");

        final String query1 = "SELECT ('dd'+'ff') as val1 FROM table1";
        HQuery<HRecord> q1 = conn.newHQuery(query1);
        List<HRecord> recList1 = q1.getResultList();
        assertTrue(recList1.size() == 10);

        int i = 0;
        for (final HRecord rec : recList1) {
            String val = (String)rec.getCurrentValue("val1");
            assertTrue(val.equals("ddff"));

            Map<Long, Object> versions = rec.getVersionMap("val1");
            assertTrue(versions == null || versions.size() == 0);

            i++;
        }
    }

    @Test
    public void selectUnknownMapExpressions() throws HBqlException, IOException {

        SchemaManager.execute("drop schema table1");

        SchemaManager.execute("define schema table1 alias tab1"
                              + "("
                              + "keyval key, "
                              + "f3:* alias f1default "
                              + ")");

        final String query1 = "SELECT f3:* FROM table1";
        HQuery<HRecord> q1 = conn.newHQuery(query1);
        List<HRecord> recList1 = q1.getResultList();
        assertTrue(recList1.size() == 10);
        for (final HRecord rec : recList1) {
            Map<String, Map<String, byte[]>> val = rec.getFamilyDefaultKeysAsColumnsMap("f3:*");
            assertTrue(val.size() == 2);

            Map<String, Map<String, NavigableMap<Long, byte[]>>> val2 = rec.getFamilyDefaultKeysAsColumnsVersionMap("f3:*");
            assertTrue(val2.size() == 2);
        }

        final String query2 = "SELECT * FROM table1";
        HQuery<HRecord> q2 = conn.newHQuery(query2);
        List<HRecord> recList2 = q2.getResultList();
        assertTrue(recList2.size() == 10);

        for (final HRecord rec : recList2) {
            Map map1 = rec.getFamilyDefaultValueMap("f3:*");
            Map map2 = rec.getFamilyDefaultKeysAsColumnsMap("f3:*");

            assertTrue(map1.size() == 3);
            assertTrue(map2.size() == 2);
        }
    }

    @Test
    public void selectUnnamedExpressions() throws HBqlException, IOException {

        SchemaManager.execute("drop schema table1");

        SchemaManager.execute("define schema table1 alias tab1"
                              + "("
                              + "keyval key, "
                              + "f1:val1 string alias val1, "
                              + "f1:val10 string alias val10, "
                              + "f1:* alias f1default "
                              + ")");

        final String query1 = "SELECT 2+4, 5+9, 5+3 as expr1, DEFINEDINROW(val1), DEFINEDINROW(val10) FROM table1";
        HQuery<HRecord> q1 = conn.newHQuery(query1);
        List<HRecord> recList1 = q1.getResultList();
        assertTrue(recList1.size() == 10);
        for (final HRecord rec : recList1) {
            int val1 = (Integer)rec.getCurrentValue(":expr-0");
            assertTrue(val1 == 6);
            int val2 = (Integer)rec.getCurrentValue(":expr-1");
            assertTrue(val2 == 14);
            int val3 = (Integer)rec.getCurrentValue("expr1");
            assertTrue(val3 == 8);

            boolean val4 = (Boolean)rec.getCurrentValue(":expr-2");
            assertTrue(val4);
            boolean val5 = (Boolean)rec.getCurrentValue(":expr-3");
            assertTrue(!val5);
        }
    }

    @Test
    public void selectEvalExpressions() throws HBqlException, IOException {

        SchemaManager.execute("drop schema table1");

        SchemaManager.execute("define schema table1 alias tab1"
                              + "("
                              + "keyval key, "
                              + "f1:val1 string alias val1, "
                              + "f1:val10 string alias val10, "
                              + "f1:* alias f1default "
                              + ")");

        final String query1 = "SELECT EVAL('TRUE'), EVAL('FALSE') FROM table1";
        HQuery<HRecord> q1 = conn.newHQuery(query1);
        List<HRecord> recList1 = q1.getResultList();
        assertTrue(recList1.size() == 10);
        for (final HRecord rec : recList1) {
            boolean val1 = (Boolean)rec.getCurrentValue(":expr-0");
            assertTrue(val1);
            boolean val2 = (Boolean)rec.getCurrentValue(":expr-1");
            assertTrue(!val2);
        }

        final String query2 = "SELECT EVAL(:val1), EVAL(:val2) FROM table1";
        HQuery<HRecord> q2 = conn.newHQuery(query2);
        q2.setParameter("val1", "TRUE OR FALSE");
        q2.setParameter("val2", "TRUE AND FALSE");
        List<HRecord> recList2 = q2.getResultList();
        assertTrue(recList2.size() == 10);
        for (final HRecord rec : recList2) {
            boolean val1 = (Boolean)rec.getCurrentValue(":expr-0");
            assertTrue(val1);
            boolean val2 = (Boolean)rec.getCurrentValue(":expr-1");
            assertTrue(!val2);
        }
    }


    @Test
    public void selectDefaults() throws HBqlException, IOException {

        SchemaManager.execute("drop schema table1");
        SchemaManager.execute("define schema table1 alias tab1"
                              + "("
                              + "keyval key, "
                              + "f1:val1 string alias val1, "
                              + "f1:val10 string alias val10 default 'test default', "
                              + "f1:val11 string alias val11 , "
                              + "f1:* alias f1default "
                              + ")");

        final String query1 = "SELECT * FROM table1";
        HQuery<HRecord> q1 = conn.newHQuery(query1);
        List<HRecord> recList1 = q1.getResultList();
        assertTrue(recList1.size() == 10);
        for (final HRecord rec : recList1) {
            String val1 = (String)rec.getCurrentValue("val10");
            assertTrue(val1.equals("test default"));
            String val2 = (String)rec.getCurrentValue("val11");
            assertTrue(val2 == null);
        }

        final String query2 = "SELECT * FROM table1 with client filter where val10 = 'test default'";
        HQuery<HRecord> q2 = conn.newHQuery(query2);
        List<HRecord> recList2 = q2.getResultList();
        assertTrue(recList2.size() == 10);

        final String query3 = "SELECT * FROM table1 with client filter where val11 = 'test default'";
        HQuery<HRecord> q3 = conn.newHQuery(query3);
        List<HRecord> recList3 = q3.getResultList();
        assertTrue(recList3.size() == 0);
    }

    @Test
    public void selectMismatchedDefaults() throws HBqlException, IOException {

        SchemaManager.execute("drop schema table1");

        Exception caughtException = null;
        try {
            SchemaManager.execute("define schema table1 alias tab1"
                                  + "("
                                  + "keyval key, "
                                  + "f1:val10 string alias val10 default 4"
                                  + ")");
        }
        catch (Exception e) {
            e.printStackTrace();
            caughtException = e;
        }

        assertTrue(caughtException instanceof TypeException);
    }

    @Test
    public void selectInvalidDefaults() throws HBqlException, IOException {

        SchemaManager.execute("drop schema table1");

        Exception caughtException = null;
        try {
            SchemaManager.execute("define schema table1 alias tab1"
                                  + "("
                                  + "keyval key, "
                                  + "f1:val10 object alias val10 default 4 "
                                  + ")");
        }
        catch (Exception e) {
            e.printStackTrace();
            caughtException = e;
        }

        assertTrue(caughtException instanceof HBqlException);
    }
}