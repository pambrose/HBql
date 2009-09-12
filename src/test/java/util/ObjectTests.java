package util;

import com.imap4j.hbase.collection.ObjectQuery;
import com.imap4j.hbase.collection.ObjectQueryListenerAdapter;
import com.imap4j.hbase.hbase.HPersistException;

import java.util.Collection;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 23, 2009
 * Time: 4:49:02 PM
 */
public class ObjectTests<T> {

    protected void assertResultCount(final Collection<T> objList, final String expr, final int expected_cnt) throws HPersistException {

        final Counter cnt = new Counter();

        final ObjectQuery<T> query = ObjectQuery.newObjectQuery(expr);
        query.addListener(new ObjectQueryListenerAdapter<T>() {
            public void onEachObject(final T val) throws HPersistException {
                cnt.increment();
            }
        }
        );

        query.execute(objList);

        System.out.println("Count = " + cnt.getCount());

        org.junit.Assert.assertTrue(expected_cnt == cnt.getCount());
    }

    protected void assertTrue(final boolean cond) {
        org.junit.Assert.assertTrue(cond);

    }

}