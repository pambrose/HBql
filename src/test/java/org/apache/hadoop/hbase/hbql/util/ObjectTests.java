package org.apache.hadoop.hbase.hbql.util;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.object.client.ObjectQuery;
import org.apache.hadoop.hbase.hbql.query.object.client.ObjectQueryManager;
import org.apache.hadoop.hbase.hbql.query.object.client.ObjectResults;

import java.util.Collection;

public class ObjectTests<T> {

    protected void assertResultCount(final Collection<T> objList, final String expr, final int expected_cnt) throws HBqlException {

        final ObjectQuery<T> query = ObjectQueryManager.newObjectQuery(expr);

        int cnt = 0;
        ObjectResults<T> results = query.execute(objList);
        for (final T val : results)
            cnt++;

        System.out.println("Count = " + cnt);

        org.junit.Assert.assertTrue(expected_cnt == cnt);
    }

    protected void assertTrue(final boolean cond) {
        org.junit.Assert.assertTrue(cond);
    }
}