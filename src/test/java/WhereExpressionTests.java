import com.imap4j.hbase.hql.HColumn;
import com.imap4j.hbase.hql.HFamily;
import com.imap4j.hbase.hql.HPersistException;
import com.imap4j.hbase.hql.HPersistable;
import com.imap4j.hbase.hql.HTable;
import com.imap4j.hbase.hql.HTest;
import org.junit.Test;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 27, 2009
 * Time: 2:13:47 PM
 */
public class WhereExpressionTests extends HTest {

    @HTable(name = "alltypes",
            families = {
                    @HFamily(name = "family1", maxVersions = 10),
                    @HFamily(name = "family2"),
                    @HFamily(name = "family3", maxVersions = 5)
            })
    public static class AllTypes implements HPersistable {

        @HColumn(key = true)
        private String keyval = null;

        @HColumn(family = "family1")
        private int intValue = -1;

        @HColumn(family = "family1")
        private String stringValue = "";

        public AllTypes() {
        }

        public AllTypes(final String keyval, final int intValue, final String stringValue) {
            this.keyval = keyval;
            this.intValue = intValue;
            this.stringValue = stringValue;
        }
    }

    @Test
    public void booleanExpressions() throws HPersistException {

        assertTrue("TRUE");
        assertFalse("FALSE");
        assertTrue("TRUE OR TRUE");
        assertTrue("TRUE OR TRUE OR TRUE");
        assertFalse("FALSE OR FALSE OR FALSE");
        assertFalse("(FALSE OR FALSE OR FALSE)");
        assertFalse("((((FALSE OR FALSE OR FALSE))))" + " OR " + "((((FALSE OR FALSE OR FALSE))))");
        assertTrue("TRUE OR FALSE");
        assertFalse("FALSE OR FALSE");
        assertTrue("TRUE AND TRUE");
        assertFalse("TRUE AND FALSE");
        assertTrue("TRUE OR ((true) or true) OR FALSE");
        assertFalse("(false AND ((true) OR true)) AND TRUE");
        assertTrue("(false AND ((true) OR true)) OR TRUE");
    }

    @Test
    public void numericCompares() throws HPersistException {

        assertTrue("4 < 5");
        assertFalse("4 = 5");
        assertFalse("4 == 5");
        assertTrue("4 != 5");
        assertTrue("4 <> 5");
        assertTrue("4 <= 5");
        assertFalse("4 > 5");
        assertFalse("4 >= 5");
    }

    @Test
    public void stringCompares() throws HPersistException {

        assertTrue("'aaa' == 'aaa'");
        assertFalse("'aaa' != 'aaa'");
        assertFalse("'aaa' <> 'aaa'");
        assertFalse("'aaa' == 'bbb'");
        assertTrue("'aaa' <= 'bbb'");
        assertTrue("'bbb' <= 'bbb'");
        assertFalse("'bbb' <= 'aaa'");
        assertFalse("'bbb' > 'bbb'");
        assertTrue("'bbb' > 'aaa'");
        assertTrue("'bbb' >= 'aaa'");
        assertTrue("'aaa' >= 'aaa'");
    }

    @Test
    public void numericCalculations() throws HPersistException {

        assertTrue("9 == 9");
        assertTrue("((4 + 5) == 9)");
        assertTrue("(9) == 9");
        assertTrue("(4 + 5) == 9");
        assertFalse("(4 + 5) == 8");
        assertTrue("(4 + 5 + 10 + 10 - 20) == 9");
        assertFalse("(4 + 5 + 10 + 10 - 20) != 9");
    }

    @Test
    public void numericFunctions() throws HPersistException {

        assertTrue("3 between 2 AND 5");
        assertTrue("3 between (1+1) AND (3+2)");

        assertTrue("3 in (2,3,4)");
        assertFalse("3 in (1+1,1+3,4)");
        assertTrue("3 in (1+1,1+2,4)");

    }

    @Test
    public void stringFunctions() throws HPersistException {

        assertTrue("'bbb' between 'aaa' AND 'ccc'");
        assertTrue("'bbb' between 'bbb' AND 'ccc'");
        assertFalse("'bbb' between 'ccc' AND 'ddd'");

        assertTrue("('bbb' between 'bbb' AND 'ccc') AND ('fff' between 'eee' AND 'ggg')");
    }

    @Test
    public void objectFunctions() throws HPersistException {

        final AllTypes obj = new AllTypes("aaa", 3, "bbb");

        assertTrue(obj, "stringValue between 'aaa' AND 'ccc'");
        assertTrue(obj, "stringValue between 'bbb' AND 'ccc'");
        assertFalse(obj, "stringValue between 'ccc' AND 'ddd'");
        assertTrue(obj, "('bbb' between stringValue AND 'ccc') AND ('fff' between 'eee' AND 'ggg')");

        assertTrue(obj, "intValue between 2 AND 5");
        assertTrue(obj, "intValue between (1+1) AND (intValue+2)");
    }

}

