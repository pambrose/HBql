package org.apache.hadoop.hbase.contrib.hbql.util;

import org.apache.expreval.util.Lists;
import org.apache.hadoop.hbase.contrib.hbql.AnnotatedAllTypes;
import org.apache.hadoop.hbase.contrib.hbql.client.Batch;
import org.apache.hadoop.hbase.contrib.hbql.client.Connection;
import org.apache.hadoop.hbase.contrib.hbql.client.ConnectionManager;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.Query;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class AnnotationInsertTest extends TestSupport {

    static Connection conn = null;

    static List<AnnotatedAllTypes> vals = null;

    static int cnt = 10;

    //@BeforeClass
    public static void onetimeSetup() throws HBqlException, IOException {

        conn = ConnectionManager.newConnection();

        if (!conn.tableExists("alltypes"))
            System.out.println(conn.execute("create table using schema AnnotatedAllTypes"));
        else {
            System.out.println(conn.execute("delete from AnnotatedAllTypes"));
        }

        vals = insertSomeData(cnt);
    }

    public static List<AnnotatedAllTypes> insertSomeData(int cnt) throws HBqlException, IOException {

        List<AnnotatedAllTypes> retval = Lists.newArrayList();
        final Batch batch = new Batch();

        for (int i = 0; i < cnt; i++) {

            AnnotatedAllTypes aat = new AnnotatedAllTypes();
            aat.setATestValue(i);

            retval.add(aat);

            batch.insert(aat);
        }

        conn.apply(batch);

        return retval;
    }


    @Test
    public void simpleSelect() throws HBqlException, IOException {

        onetimeSetup();

        assertTrue(vals.size() == cnt);

        Query<AnnotatedAllTypes> recs = conn.newQuery("select * from AnnotatedAllTypes");
        List<AnnotatedAllTypes> recList = recs.getResultList();

        assertTrue(recList.size() == cnt);

        for (int i = 0; i < cnt; i++)
            assertTrue(recList.get(i).equals(vals.get(i)));
    }
}
