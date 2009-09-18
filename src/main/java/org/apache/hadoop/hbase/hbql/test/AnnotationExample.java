package org.apache.hadoop.hbase.hbql.test;

import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.client.HQuery;
import org.apache.hadoop.hbase.hbql.client.HResults;
import org.apache.hadoop.hbase.hbql.client.HTransaction;

import java.io.IOException;
import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 17, 2009
 * Time: 8:53:41 PM
 */
public class AnnotationExample {

    public static void main(String[] args) throws IOException, HPersistException {

        HConnection conn = HConnection.newHConnection();

        /*
        if (conn.tableExists("TestObject")) {
            System.out.println(conn.exec("disable table TestObject"));
            System.out.println(conn.exec("drop table TestObject"));
        }

        if (!conn.tableExists("TestObject"))
            System.out.println(conn.exec("create table using TestObject"));
        */

        final HTransaction tx = new HTransaction();
        for (int i = 0; i < 10; i++)
            tx.insert(new TestObject(i));

        conn.apply(tx);

        final String query2 = "SELECT title, titles, author, authorVersions "
                              + "FROM TestObject "
                              + "WITH "
                              + "KEYS  ALL "
                              //+ "KEYS  '000002' TO '000005', '000007' TO LAST "
                              + "TIME RANGE NOW()-DAY(15) TO NOW()+DAY(1)"
                              + "VERSIONS MAX "
                              + "SERVER FILTER WHERE author LIKE '.*val.*'"
                //+ "CLIENT FILTER WHERE author LIKE '.*val.*'"
                ;

        HQuery<TestObject> q2 = conn.newHQuery(query2);
        HResults<TestObject> results2 = q2.execute();

        for (TestObject val2 : results2) {
            System.out.println("Current Values: " + val2.keyval + " - " + val2.author + " - " + val2.title);

            System.out.println("Historicals");

            if (val2.authorVersions != null)
                for (final Long key : val2.authorVersions.keySet())
                    System.out.println(new Date(key) + " - " + val2.authorVersions.get(key));

            if (val2.titles != null)
                for (final Long key : val2.titles.keySet())
                    System.out.println(new Date(key) + " - " + val2.titles.get(key));
        }

        results2.close();
    }

}
