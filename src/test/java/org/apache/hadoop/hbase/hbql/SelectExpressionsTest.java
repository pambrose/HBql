package org.apache.hadoop.hbase.hbql;

import org.apache.hadoop.hbase.hbql.client.HBatch;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
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
    static List<Integer> val5List = Lists.newArrayList();
    static List<Integer> val7List = Lists.newArrayList();

    static Random randomVal = new Random();

    @BeforeClass
    public static void ontimeSetup() throws HBqlException, IOException {

        SchemaManager.parse("define table table1 alias tab1"
                            + "("
                            + "keyval key, "
                            + "f1:val1 string alias val1, "
                            + "f1:val2 string alias val2, "
                            + "f2:val1 date alias val3, "
                            + "f2:val2 date alias val4, "
                            + "f3:val1 int alias val5, "
                            + "f3:val2 int alias val6, "
                            + "f3:val3 int alias val7"
                            + ")");

        conn = HConnection.newHConnection();

        if (!conn.tableExists("table1"))
            System.out.println(conn.execute("create table using table1"));
        else
            System.out.println(conn.execute("delete from table1"));

        final HBatch batch = new HBatch();
        for (int i = 0; i < 10; i++) {
            final HRecord rec = new HRecord("table1");
            final String keyval = HUtil.getZeroPaddedNumber(i, 10);
            keyList.add(keyval);
            rec.setCurrentValue("keyval", keyval);

            int val5 = randomVal.nextInt();
            val5List.add(val5);
            rec.setCurrentValue("val5", val5);
            rec.setCurrentValue("val6", i * 100);
            batch.insert(rec);
        }

        conn.apply(batch);

    }

    @Test
    public void selectExpressions() throws HBqlException, IOException {

        final String query1 = "SELECT val5, (val5 - val5 + val5) as val6 FROM table1";

        HQuery<HRecord> q1 = conn.newHQuery(query1);
        HResults<HRecord> results1 = q1.execute();

        List<String> testKeyVals = Lists.newArrayList();
        List<Integer> testVal5Vals = Lists.newArrayList();

        for (HRecord rec : results1) {

            String keyval = (String)rec.getCurrentValue("keyval");
            int val5 = (Integer)rec.getCurrentValue("val5");
            long val6 = (Integer)rec.getCurrentValue("val6");

            testKeyVals.add(keyval);
            testVal5Vals.add(val5);

            assertTrue(val5 == val6);

            System.out.println("Current Values: " + keyval
                               + " - " + rec.getCurrentValue("val5")
                               + " - " + rec.getCurrentValue("val6")
            );

        }

        assertTrue(testKeyVals.equals(keyList));
        assertTrue(testVal5Vals.equals(val5List));

    }

}