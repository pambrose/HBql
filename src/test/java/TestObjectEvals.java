import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.imap4j.hbase.collection.CollectionQuery;
import com.imap4j.hbase.collection.CollectionQueryListenerAdapter;
import com.imap4j.hbase.collection.CollectionQueryPredicate;
import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.test.ObjectTests;
import org.junit.Test;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 27, 2009
 * Time: 2:13:47 PM
 */
public class TestObjectEvals extends ObjectTests<TestObjectEvals.SimpleObject> {

    public class SimpleObject {
        final int intval1, intval2;
        final String strval;

        public SimpleObject(final int val) {
            this.intval1 = val;
            this.intval2 = this.intval1 * 2;
            this.strval = "Test Value: " + val;
        }

        public String toString() {
            return "intval1: " + intval1 + " intval2: " + intval2 + " strval: " + strval;
        }

    }

    @Test
    public void objectExpressions() throws HPersistException {

        final List<SimpleObject> objList = Lists.newArrayList();
        for (int i = 0; i < 10; i++)
            objList.add(new SimpleObject(i));

        assertResultCount(objList, "intval1 == 2", 1);
        assertResultCount(objList, "intval1 >= 5", 5);
        assertResultCount(objList, "2*intval1 == intval2", 10);
        assertResultCount(objList, "intval2 == 2*intval1", 10);
        assertResultCount(objList, "intval1 between 1 and 4", 4);
        assertResultCount(objList, "intval1 in (1, 2+1, 2+1+1, 4+3)", 4);
        assertResultCount(objList, "strval LIKE 'Test Value: [1-5]'", 5);

        final CollectionQuery<SimpleObject> query = new CollectionQuery<SimpleObject>(
                "intval1 == 2",
                new CollectionQueryListenerAdapter<SimpleObject>() {
                    public void onEachObject(final SimpleObject val) throws HPersistException {
                        System.out.println(val);
                    }
                }
        );
        query.execute(objList);

        // Using Google collections
        String qstr = "intval1 in (1, 2+1, 2+1+1, 4+3)";
        List<SimpleObject> list = Lists.newArrayList(Iterables.filter(objList, new CollectionQueryPredicate<SimpleObject>(qstr)));
        System.out.println(list.size());
    }

}