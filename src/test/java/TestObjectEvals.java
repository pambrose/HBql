import com.google.common.collect.Lists;
import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.test.ObjectTests;
import org.junit.Test;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 27, 2009
 * Time: 2:13:47 PM
 */
public class TestObjectEvals extends ObjectTests {

    public class SimpleObject {
        final int intval1, intval2;
        final String strval;

        public SimpleObject(final int val) {
            this.intval1 = val;
            this.intval2 = this.intval1 * 2;
            this.strval = "Test Value: " + val;
        }
    }

    @Test
    public void booleanExpressions() throws HPersistException, NoSuchMethodException {

        final List<SimpleObject> objList = Lists.newArrayList();
        for (int i = 0; i < 10; i++)
            objList.add(new SimpleObject(i));

        assertResultCount(objList, "intval1 == 2", 1);
        assertResultCount(objList, "intval1 >= 5", 5);
        assertResultCount(objList, "2*intval1 == intval2", 10);
        assertResultCount(objList, "intval2 == 2*intval1", 10);
    }

}