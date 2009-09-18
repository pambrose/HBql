package org.apache.hadoop.hbase.hbql.test;

import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.client.HQuery;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HResults;
import org.apache.hadoop.hbase.hbql.client.HTransaction;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 19, 2009
 * Time: 4:39:06 PM
 */
public class HRecordExample {


    public static void main(String[] args) throws IOException, HPersistException {

        HConnection conn = HConnection.newHConnection();

        SchemaManager.exec("define table testobjects alias testobjects2"
                           + "("
                           + "keyval key, "
                           + "family1:author string alias author, "
                           + "family1:title string  alias title"
                           + ")");

        // System.out.println(conn.exec("delete from TestObject with client filter where true"));
        // System.out.println(conn.exec("disable table testobjects"));
        // System.out.println(conn.exec("enable table testobjects"));
        // System.out.println(conn.exec("drop table testobjects"));

        System.out.println(conn.exec("show tables"));

        if (!conn.tableExists("testobjects"))
            System.out.println(conn.exec("create table using testobjects"));

        if (conn.tableEnabled("testobjects2"))
            System.out.println(conn.exec("describe table testobjects2"));

        final HTransaction tx = new HTransaction();
        for (int i = 0; i < 10; i++) {
            HRecord hrecord = new HRecord("testobjects");
            hrecord.setCurrentValue("keyval", HUtil.getZeroPaddedNumber(i, 10));
            hrecord.setCurrentValue("author", "A new author value: " + i);
            hrecord.setCurrentValue("title", "A very new title value: " + i);
            tx.insert(hrecord);
        }
        // Make sure key value is set
        conn.apply(tx);

        final String query1 = "SELECT author, title "
                              + "FROM testobjects2 "
                              + "WITH "
                              + "KEYS ALL "
                              + "TIME RANGE NOW()-DAY(15) TO NOW()+DAY(1)"
                              + "VERSIONS MAX "
                //+ "SCAN LIMIT 4"
                //+ "SERVER FILTER WHERE author LIKE '.*6200.*' "
                //+ "CLIENT FILTER WHERE family1:author LIKE '.*282.*'"
                ;
        HQuery<HRecord> q1 = conn.newHQuery(query1);
        HResults<HRecord> results1 = q1.execute();

        for (HRecord val1 : results1) {
            System.out.println("Current Values: " + val1.getCurrentValue("keyval")
                               + " - " + val1.getCurrentValue("family1:author")
                               + " - " + val1.getCurrentValue("title"));

            System.out.println("Historicals");

            if (val1.getVersionedValueMap("author") != null) {
                Map<Long, Object> versioned = val1.getVersionedValueMap("family1:author");
                for (final Long key : versioned.keySet())
                    System.out.println(new Date(key) + " - " + versioned.get(key));
            }

            if (val1.getVersionedValueMap("family1:title") != null) {
                Map<Long, Object> versioned = val1.getVersionedValueMap("title");
                for (final Long key : versioned.keySet())
                    System.out.println(new Date(key) + " - " + versioned.get(key));
            }
        }

        results1.close();
    }

}