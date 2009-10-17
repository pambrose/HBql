package org.apache.hadoop.hbase.hbql.examples;

import org.apache.hadoop.hbase.hbql.client.HBatch;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HConnectionManager;
import org.apache.hadoop.hbase.hbql.client.HQuery;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HResults;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.hbql.query.util.HUtil;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

public class HRecordExample {

    public static void main(String[] args) throws IOException, HBqlException {

        SchemaManager.parse("define table testobjects alias testobjects2"
                            + "("
                            + "keyval key, "
                            + "family1:author string alias author, "
                            + "family1:title string  alias title, "
                            + "family1:intValue int alias comp1"
                            + "f3:mapval1 string mapKeysAsColumns alias f3mapval1, "
                            + "f3:mapval2 string mapKeysAsColumns alias f3mapval2 "
                            + ")");

        HConnection conn = HConnectionManager.newHConnection();

        // System.out.println(conn.execute("delete from TestObject with client filter where true"));
        // System.out.println(conn.execute("disable table testobjects"));
        // System.out.println(conn.execute("enable table testobjects"));
        // System.out.println(conn.execute("drop table testobjects"));

        System.out.println(conn.execute("show tables"));

        if (!conn.tableExists("testobjects")) {
            System.out.println(conn.execute("create table using testobjects"));

            final HBatch batch = new HBatch();
            for (int i = 0; i < 10; i++) {
                HRecord hrecord = SchemaManager.newHRecord("testobjects");
                hrecord.setValue("keyval", HUtil.getZeroPaddedNumber(i, 10));
                hrecord.setValue("author", "A new author value: " + i);
                hrecord.setValue("title", "A very new title value: " + i);
                batch.insert(hrecord);
            }

            conn.apply(batch);
        }

        if (conn.tableEnabled("testobjects2"))
            System.out.println(conn.execute("describe table testobjects2"));

        final String query1 = "SELECT keyval, author, title, (3*12) as comp1 "
                              + "FROM testobjects2 "
                              + "WITH "
                              + "KEYS '0000000002' TO '0000000003', '0000000008' TO LAST "
                              + "TIME RANGE NOW()-DAY(25) TO NOW()+DAY(1)"
                              + "VERSIONS 2 "
                              //+ "SCAN LIMIT 4"
                              //+ "SERVER FILTER WHERE author LIKE '.*6200.*' "
                              + "CLIENT FILTER WHERE keyval = '0000000002' OR author LIKE '.*val.*'";
        HQuery<HRecord> q1 = conn.newHQuery(query1);
        HResults<HRecord> results1 = q1.getResults();

        for (HRecord val1 : results1) {
            System.out.println("Current Values: " + val1.getValue("keyval")
                               + " - " + val1.getValue("family1:author")
                               + " - " + val1.getValue("title")
                               + " - " + val1.getValue("comp1")
            );

            System.out.println("Historicals");

            if (val1.getVersionMap("author") != null) {
                Map<Long, Object> versioned = val1.getVersionMap("family1:author");
                for (final Long key : versioned.keySet())
                    System.out.println(new Date(key) + " - " + versioned.get(key));
            }

            if (val1.getVersionMap("family1:title") != null) {
                Map<Long, Object> versioned = val1.getVersionMap("title");
                for (final Long key : versioned.keySet())
                    System.out.println(new Date(key) + " - " + versioned.get(key));
            }
        }
        results1.close();
    }
}