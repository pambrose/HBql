package org.apache.hadoop.hbase.hbql;

import org.apache.hadoop.hbase.hbql.client.HBatch;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HQuery;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HResults;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.hbql.query.util.HUtil;
import org.apache.hadoop.hbase.hbql.util.TestSupport;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 27, 2009
 * Time: 2:13:47 PM
 */
public class SelectExpressionsTest extends TestSupport {

    static HConnection conn = null;

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
                            + "f3:val2 int alias val6"
                            + ")");

        conn = HConnection.newHConnection();

        if (!conn.tableExists("table1"))
            System.out.println(conn.execute("create table using table1"));
        else
            System.out.println(conn.execute("delete from table1"));

        final HBatch batch = new HBatch();
        for (int i = 0; i < 10; i++) {
            final HRecord rec = new HRecord("table1");
            rec.setCurrentValue("keyval", HUtil.getZeroPaddedNumber(i, 10));
            rec.setCurrentValue("val5", i * 12);
            rec.setCurrentValue("val6", i * 15);
            batch.insert(rec);
        }

        conn.apply(batch);

    }

    @Test
    public void selectExpressions() throws HBqlException, IOException {

        assertSelectElementsMatchTrue("SELECT f1:val1 FROM table1", "f1:val1");

        final String query1 = "SELECT val5, val6 "
                              + "FROM table1 "
                              + "WITH "
                              // + "KEYS '0000000002' TO '0000000003', '0000000008' TO LAST "
                              // + "TIME RANGE NOW()-DAY(15) TO NOW()+DAY(1)"
                              // + "VERSIONS 2 "
                              //+ "SCAN LIMIT 4"
                              + "CLIENT FILTER WHERE TRUE";

        HQuery<HRecord> q1 = conn.newHQuery(query1);
        HResults<HRecord> results1 = q1.execute();

        for (HRecord rec : results1) {
            System.out.println("Current Values: " + rec.getCurrentValue("keyval")
                               + " - " + rec.getCurrentValue("val5")
                               + " - " + rec.getCurrentValue("val6")
            );

        }

    }

}