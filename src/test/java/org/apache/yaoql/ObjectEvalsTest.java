package org.apache.yaoql;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.expreval.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.util.Counter;
import org.apache.hadoop.hbase.contrib.hbql.util.ObjectTests;
import org.apache.yaoql.client.ObjectQuery;
import org.apache.yaoql.client.ObjectQueryListenerAdapter;
import org.apache.yaoql.client.ObjectQueryManager;
import org.apache.yaoql.client.ObjectQueryPredicate;
import org.apache.yaoql.client.ObjectResults;
import org.junit.Test;

import java.util.Date;
import java.util.List;

public class ObjectEvalsTest extends ObjectTests<ObjectEvalsTest.SimpleObject> {

    public class SimpleObject {
        final int intval1, intval2;
        final String strval;
        final Date dateval;

        public SimpleObject(final int val) {
            this.intval1 = val;
            this.intval2 = this.intval1 * 2;
            this.strval = "Test Value: " + val;
            this.dateval = new Date(System.currentTimeMillis());
        }

        public String toString() {
            return "intval1: " + intval1 + " intval2: " + intval2 + " strval: " + strval;
        }
    }

    @Test
    public void objectExpressions() throws HBqlException {

        final List<SimpleObject> objList = Lists.newArrayList();
        for (int i = 0; i < 10; i++)
            objList.add(new SimpleObject(i));

        assertResultCount(objList, "intval1 = 2", 1);
        assertResultCount(objList, "intval1 >= 5", 5);
        assertResultCount(objList, "2*intval1 = intval2", 10);
        assertResultCount(objList, "intval2 = 2*intval1", 10);
        assertResultCount(objList, "intval1 between 1 and 4", 4);
        assertResultCount(objList, "intval1 in (1, 2+1, 2+1+1, 4+3)", 4);
        assertResultCount(objList, "strval like 'T[est]+ Value: [1-5]'", 5);
        assertResultCount(objList, "NOW() between NOW()-DAY(1) AND NOW()+DAY(1)", 10);
        assertResultCount(objList, "dateval between NOW()-MINUTE(1) AND NOW()+MINUTE(1)", 10);
        assertResultCount(objList, "dateval between DATE('09/09/2009', 'mm/dd/yyyy')-MINUTE(1) AND NOW()+MINUTE(1)", 10);

        // Using Listeners with CollectionQuery Object
        String qstr = "strval like 'T[est]+ Value: [1-3]'";
        final Counter cnt1 = new Counter();
        ObjectQuery<SimpleObject> query = ObjectQueryManager.newObjectQuery(qstr);
        query.addListener(
                new ObjectQueryListenerAdapter<SimpleObject>() {
                    public void onEachObject(final SimpleObject val) throws HBqlException {
                        cnt1.increment();
                    }
                }
        );
        query.getResults(objList);
        assertTrue(cnt1.getCount() == 3);

        query = ObjectQueryManager.newObjectQuery(qstr);
        List<SimpleObject> r1 = query.getResultList(objList);
        assertTrue(r1.size() == 3);

        ObjectQuery<SimpleObject> query1 = ObjectQueryManager.newObjectQuery("strval like :str1");
        query1.setParameter("str1", "T[est]+ Value: [1-3]");
        r1 = query1.getResultList(objList);
        assertTrue(r1.size() == 3);

        // Using Iterator
        int cnt2 = 0;
        ObjectQuery<SimpleObject> query2 = ObjectQueryManager.newObjectQuery("strval like 'T[est]+ Value: [1-3]'");
        final ObjectResults<SimpleObject> results = query2.getResults(objList);
        for (final SimpleObject obj : results)
            cnt2++;
        assertTrue(cnt2 == 3);

        // Using Google collections
        qstr = "intval1 in (1, 2+1, 2+1+1, 4+3)";
        List<SimpleObject> list = Lists.newArrayList(Iterables.filter(objList, new ObjectQueryPredicate<SimpleObject>(qstr)));
        assertTrue(list.size() == 4);

        ObjectQueryPredicate<SimpleObject> pred = new ObjectQueryPredicate<SimpleObject>("intval1 in (:vals)");
        List<Integer> intList = Lists.newArrayList();
        intList.add(1);
        intList.add(3);
        intList.add(4);
        intList.add(7);
        pred.setParameter("vals", intList);

        list = Lists.newArrayList(Iterables.filter(objList, pred));
        assertTrue(list.size() == 4);
    }
}