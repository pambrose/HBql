package org.apache.hadoop.hbase.hbql.test;

import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.client.HQuery;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HResults;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 19, 2009
 * Time: 4:39:06 PM
 */
public class HRecordAccess {


    public static void main(String[] args) throws IOException, HPersistException {

        HConnection conn = HConnection.newHConnection();
        System.out.println(conn.exec("define table testobjects "
                                     + "("
                                     + "keyval key, "
                                     + "family1:author string, "
                                     + "family1:title string "
                                     + ")"));

        //System.out.println(conn.exec("delete from TestObject with client filter where true"));
        // System.out.println(conn.exec("disable table testobjects"));
        // System.out.println(conn.exec("enable table testobjects"));
        // System.out.println(conn.exec("drop table testobjects"));
        //System.out.println(conn.exec("create table using testobjects"));
        System.out.println(conn.exec("show tables"));
        System.out.println(conn.exec("describe table TestObject"));

        final String query1 = "SELECT family1:author, family1:title "
                              + "FROM testobjects "
                              + "WITH "
                // + "KEYS  '000002' TO '000005', '000007' TO LAST "
                //+ "TIME RANGE NOW()-DAY(15) TO NOW()+DAY(1)"
                //+ "VERSIONS 3 "
                // + "SCAN LIMIT 1"
                //+ "SERVER FILTER WHERE TRUE "
                //+ "SERVER FILTER WHERE family1:author LIKE '.*val.*' "
                //+ "CLIENT FILTER WHERE family1:author LIKE '.*282.*'"
                ;
        HQuery<HRecord> q1 = conn.newHQuery(query1);
        HResults<HRecord> results1 = q1.execute();

        for (HRecord val1 : results1) {
            System.out
                    .println("Current Values: " + val1.getCurrentValueByVariableName("keyval")
                             + " - " + val1.getCurrentValueByVariableName("family1:author")
                             + " - " + val1.getCurrentValueByVariableName("family1:title"));

            System.out.println("Historicals");

            if (val1.getVersionedValueMapByVariableName("family1:author") != null) {
                Map<Long, Object> versioned = val1.getVersionedValueMapByVariableName("family1:author");
                for (final Long key : versioned.keySet())
                    System.out.println(new Date(key) + " - " + versioned.get(key));
            }

            if (val1.getVersionedValueMapByVariableName("family1:title") != null) {
                Map<Long, Object> versioned = val1.getVersionedValueMapByVariableName("family1:title");
                for (final Long key : versioned.keySet())
                    System.out.println(new Date(key) + " - " + versioned.get(key));
            }
        }

        results1.close();
    }

}