package org.apache.hadoop.hbase.hbql.util;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.object.ObjectQuery;
import org.apache.hadoop.hbase.hbql.query.object.ObjectResults;

import java.util.Collection;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 23, 2009
 * Time: 4:49:02 PM
 */
public class ObjectTests<T> {

    protected void assertResultCount(final Collection<T> objList, final String expr, final int expected_cnt) throws HPersistException {

        final ObjectQuery<T> query = ObjectQuery.newObjectQuery(expr);

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