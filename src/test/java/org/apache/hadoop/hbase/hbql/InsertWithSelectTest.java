package org.apache.hadoop.hbase.hbql;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HConnectionManager;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.client.HQuery;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HResults;
import org.apache.hadoop.hbase.hbql.client.PreparedStatement;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.stmt.util.HUtil;
import org.apache.hadoop.hbase.hbql.util.TestSupport;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

public class InsertWithSelectTest extends TestSupport {

    static HConnection conn = null;

    static Random randomVal = new Random();

    @BeforeClass
    public static void onetimeSetup() throws HBqlException, IOException {

        SchemaManager.execute("CREATE SCHEMA tab3 FOR TABLE table3"
                              + "("
                              + "keyval key, "
                              + "f1:val1 string alias val1, "
                              + "f1:val2 int alias val2 "
                              + ")");

        conn = HConnectionManager.newHConnection();

        if (!conn.tableExists("table3"))
            System.out.println(conn.execute("create table using schema tab3"));
        else {
            System.out.println(conn.execute("delete from tab3"));
            //System.out.println(conn.execute("disable table table3"));
            //System.out.println(conn.execute("drop table table3"));
            //System.out.println(conn.execute("create table using schema tab3"));
        }

        insertRecords(conn, 10);
    }

    private static void insertRecords(final HConnection conn,
                                      final int cnt) throws HBqlException, IOException {

        PreparedStatement stmt = conn.prepare("insert into tab3 " +
                                              "(keyval, val1, val2) values " +
                                              "(:key, :val1, :val2)");

        for (int i = 0; i < cnt; i++) {

            int val = 10 + i;

            final String keyval = HUtil.getZeroPaddedNumber(i, 10);

            stmt.setParameter("key", keyval);
            stmt.setParameter("val1", "" + val);
            stmt.setParameter("val2", val);
            stmt.execute();
        }
    }

    private static void showValues() throws HBqlException, IOException {

        final String query1 = "SELECT keyval, val1, val2 FROM tab3";

        HQuery<HRecord> q1 = conn.newHQuery(query1);

        HResults<HRecord> results = q1.getResults();

        int rec_cnt = 0;
        for (HRecord rec : results) {

            String keyval = (String)rec.getCurrentValue("keyval");
            String val1 = (String)rec.getCurrentValue("val1");
            int val2 = (Integer)rec.getCurrentValue("val2");

            System.out.println("Current Values: " + keyval + " : " + val1 + " : " + val2);
            rec_cnt++;
        }

        assertTrue(rec_cnt == 10);
    }

    @Test
    public void insertWithSelect() throws HBqlException, IOException {

        final String q1 = "insert into tab3 " +
                          "(keyval, val1, val2) " +
                          "select keyval, val1+val1, val2+1 FROM tab3 ";
        showValues();

        PreparedStatement stmt = conn.prepare(q1);

        HOutput output = stmt.execute();

        System.out.println(output);

        showValues();

        output = conn.execute(q1);

        System.out.println(output);

        showValues();
    }

    private Class<? extends Exception> execute(final String str) {

        try {
            conn.execute(str);
        }
        catch (Exception e) {
            e.printStackTrace();
            return e.getClass();
        }
        return null;
    }

    @Test
    public void insertTypeExceptions() throws HBqlException, IOException {

        Class<? extends Exception> caught;

        caught = this.execute("insert into tab3 " +
                              "(keyval, val1, val2) " +
                              "select keyval, DOUBLE(val1+val1), val2+1 FROM tab3 ");
        assertTrue(caught != null && caught == TypeException.class);

        caught = this.execute("insert into tab3 " +
                              "(keyval, val1, val2) " +
                              "select keyval, val2, val1 FROM tab3 ");
        assertTrue(caught != null && caught == TypeException.class);

        caught = this.execute("insert into tab3 " +
                              "(keyval, val1, val2) " +
                              "select keyval, val2 FROM tab3 ");
        assertTrue(caught != null && caught == HBqlException.class);

        caught = this.execute("insert into tab3 " +
                              "(keyval, val1, val2) " +
                              "values ('123', 'aaa', 'ss') ");
        assertTrue(caught != null && caught == TypeException.class);

        caught = this.execute("insert into tab3 " +
                              "(keyval, val1, val2) " +
                              "values (4, 'aaa', 5) ");
        assertTrue(caught != null && caught == TypeException.class);
    }
}