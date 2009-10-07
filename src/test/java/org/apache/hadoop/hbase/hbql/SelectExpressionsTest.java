package org.apache.hadoop.hbase.hbql;

import org.apache.hadoop.hbase.hbql.client.HBatch;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HConnectionManager;
import org.apache.hadoop.hbase.hbql.client.HQuery;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HResults;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.hbql.query.util.HUtil;
import org.apache.hadoop.hbase.hbql.query.util.Lists;
import org.apache.hadoop.hbase.hbql.util.TestSupport;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 27, 2009
 * Time: 2:13:47 PM
 */
public class SelectExpressionsTest extends TestSupport {

    static HConnection conn = null;
    static List<String> keyList = Lists.newArrayList();
    static List<String> val1List = Lists.newArrayList();
    static List<Integer> val5List = Lists.newArrayList();
    static List<Integer> val7List = Lists.newArrayList();

    static Random randomVal = new Random();

    @BeforeClass
    public static void onetimeSetup() throws HBqlException, IOException {

        SchemaManager.parse("define table table1 alias tab1"
                            + "("
                            + "keyval key, "
                            + "f1:val1 string alias val1, "
                            + "f1:val2 string alias val2, "
                            + "f2:val1 date alias val3, "
                            + "f2:val2 date alias val4, "
                            + "f3:val1 int alias val5, "
                            + "f3:val2 int alias val6, "
                            + "f3:val3 int alias val7, "
                            + "f3:mapval1 string mapKeysAsColumns alias f3default"
                            + ")");

        conn = HConnectionManager.newHConnection();

        if (!conn.tableExists("table1"))
            System.out.println(conn.execute("create table using table1"));
        else
            System.out.println(conn.execute("delete from table1"));

        final HBatch batch = new HBatch();
        for (int i = 40; i < 50; i++) {

            final HRecord rec = new HRecord("table1");

            final String keyval = HUtil.getZeroPaddedNumber(i, 10);
            keyList.add(keyval);
            rec.setCurrentValue("keyval", keyval);

            int val5 = randomVal.nextInt();
            String s_val5 = "" + val5;
            val1List.add(s_val5);
            val5List.add(val5);

            rec.setCurrentValue("val1", s_val5);
            rec.setCurrentValue("val5", val5);
            rec.setCurrentValue("val6", i * 100);

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

}