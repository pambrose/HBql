package util;

import com.imap4j.hbase.collection.CollectionQuery;
import com.imap4j.hbase.collection.CollectionQueryListenerAdapter;
import com.imap4j.hbase.hbase.HPersistException;

import java.util.Collection;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 23, 2009
 * Time: 4:49:02 PM
 */
public class ObjectTests<T> {

    public class Counter {
        int count = 0;

        public void increment() {
            count++;
        }

        public int getCount() {
            return this.count;
        }
    }

    protected void assertResultCount(final Collection<T> objList, final String expr, final int expected_cnt) throws HPersistException {

        final Counter cnt = new Counter();

        final CollectionQuery<T> query = new CollectionQuery<T>(
                expr,
                new CollectionQueryListenerAdapter<T>() {
                    public void onEachObject(final T val) throws HPersistException {
                        cnt.increment();
                    }
                }
        );

        query.execute(objList);

        System.out.println("Count = " + cnt.getCount());

        org.junit.Assert.assertTrue(expected_cnt == cnt.getCount());
    }

}