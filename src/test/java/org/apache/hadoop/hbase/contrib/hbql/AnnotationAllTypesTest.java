package org.apache.hadoop.hbase.contrib.hbql;

import org.apache.expreval.util.Lists;
import org.apache.hadoop.hbase.contrib.hbql.client.Batch;
import org.apache.hadoop.hbase.contrib.hbql.client.Connection;
import org.apache.hadoop.hbase.contrib.hbql.client.ConnectionManager;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.Query;
import org.apache.hadoop.hbase.contrib.hbql.util.TestSupport;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class AnnotationAllTypesTest extends TestSupport {

    static Connection conn = null;

    static int cnt = 10;

    @BeforeClass
    public static void emptyTable() throws HBqlException, IOException {

        conn = ConnectionManager.newConnection();

        if (!conn.tableExists("alltypes"))
            System.out.println(conn.execute("create table using schema AnnotatedAllTypes"));
        else {
            System.out.println(conn.execute("delete from AnnotatedAllTypes"));
        }
    }

    public static List<AnnotatedAllTypes> insertSomeData(int cnt, boolean noRandomData) throws HBqlException, IOException {

        List<AnnotatedAllTypes> retval = Lists.newArrayList();
        final Batch batch = new Batch();

        for (int i = 0; i < cnt; i++) {

            AnnotatedAllTypes aat = new AnnotatedAllTypes();
            aat.setSomeValues(i, noRandomData, cnt);

            retval.add(aat);

            batch.insert(aat);
        }

        conn.apply(batch);

        return retval;
    }


    @Test
    public void simpleSelect() throws HBqlException, IOException {

        List<AnnotatedAllTypes> vals = insertSomeData(cnt, true);

        assertTrue(vals.size() == cnt);

        Query<AnnotatedAllTypes> recs = conn.newQuery("select * from AnnotatedAllTypes");

        int reccnt = 0;
        for (final AnnotatedAllTypes rec : recs.getResults())
            assertTrue(rec.equals(vals.get(reccnt++)));

        assertTrue(reccnt == cnt);
    }

    @Test
    public void simpleSparseSelect() throws HBqlException, IOException {

        List<AnnotatedAllTypes> vals = insertSomeData(cnt, false);

        assertTrue(vals.size() == cnt);

        Query<AnnotatedAllTypes> recs = conn.newQuery("select * from AnnotatedAllTypes");

        int reccnt = 0;
        for (final AnnotatedAllTypes rec : recs.getResults())
            assertTrue(rec.equals(vals.get(reccnt++)));

        assertTrue(reccnt == cnt);
    }

    @Test
    public void simpleLimitSelect() throws HBqlException, IOException {

        List<AnnotatedAllTypes> vals = insertSomeData(cnt, true);

        assertTrue(vals.size() == cnt);

        Query<AnnotatedAllTypes> recs = conn.newQuery("select * from AnnotatedAllTypes WITH LIMIT :limit");

        recs.setParameter("limit", cnt / 2);

        int reccnt = 0;
        for (final AnnotatedAllTypes rec : recs.getResults())
            assertTrue(rec.equals(vals.get(reccnt++)));

        assertTrue(reccnt == cnt / 2);
    }
}
