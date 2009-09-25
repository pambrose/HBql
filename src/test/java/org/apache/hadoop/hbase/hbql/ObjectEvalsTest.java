package org.apache.hadoop.hbase.hbql;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.object.ObjectQuery;
import org.apache.hadoop.hbase.hbql.query.object.ObjectQueryListenerAdapter;
import org.apache.hadoop.hbase.hbql.query.object.ObjectQueryPredicate;
import org.apache.hadoop.hbase.hbql.query.object.ObjectResults;
import org.apache.hadoop.hbase.hbql.util.Counter;
import org.apache.hadoop.hbase.hbql.util.ObjectTests;
import org.junit.Test;

import java.util.Date;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 27, 2009
 * Time: 2:13:47 PM
 */
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
        assertResultCount(objList, "dateval between DATE('mm/dd/yyyy', '09/09/2009')-MINUTE(1) AND NOW()+MINUTE(1)", 10);

        // Using Listeners with CollectionQuery Object
        final Counter cnt1 = new Counter();
        final ObjectQuery<SimpleObject> query = ObjectQuery.newObjectQuery("strval like 'T[est]+ Value: [1-3]'");
        query.addListener(
                new ObjectQueryListenerAdapter<SimpleObject>() {
                    public void onEachObject(final SimpleObject val) throws HBqlException {
                        cnt1.increment();
                    }
                }
        );
        query.execute(objList);
        assertTrue(cnt1.getCount() == 3);

        // Using Iterator
        final Counter cnt2 = new Counter();
        ObjectQuery<SimpleObject> query2 = ObjectQuery.newObjectQuery("strval like 'T[est]+ Value: [1-3]'");
        final ObjectResults<SimpleObject> results = query2.execute(objList);
        for (final SimpleObject obj : results)
            cnt2.increment();
        assertTrue(cnt2.getCount() == 3);

        // Using Google collections
        String qstr = "intval1 in (1, 2+1, 2+1+1, 4+3)";
        List<SimpleObject> list = Lists.newArrayList(Iterables.filter(objList, new ObjectQueryPredicate<SimpleObject>(qstr)));
        assertTrue(list.size() == 4);
    }
}