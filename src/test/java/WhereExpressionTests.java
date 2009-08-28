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
public class WhereExpressionTests {

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

        HTest.assertTrue("TRUE");
        HTest.assertFalse("FALSE");
        HTest.assertTrue("TRUE OR TRUE");
        HTest.assertTrue("TRUE OR TRUE OR TRUE");
        HTest.assertFalse("FALSE OR FALSE OR FALSE");
        HTest.assertFalse("(FALSE OR FALSE OR FALSE)");
        HTest.assertFalse("((((FALSE OR FALSE OR FALSE))))" + " OR " + "((((FALSE OR FALSE OR FALSE))))");
        HTest.assertTrue("TRUE OR FALSE");
        HTest.assertFalse("FALSE OR FALSE");
        HTest.assertTrue("TRUE AND TRUE");
        HTest.assertFalse("TRUE AND FALSE");
        HTest.assertTrue("TRUE OR ((true) or true) OR FALSE");
        HTest.assertFalse("(false AND ((true) OR true)) AND TRUE");
        HTest.assertTrue("(false AND ((true) OR true)) OR TRUE");
    }

    @Test
    public void numericCompares() throws HPersistException {

        HTest.assertTrue("4 < 5");
        HTest.assertFalse("4 = 5");
        HTest.assertFalse("4 == 5");
        HTest.assertTrue("4 != 5");
        HTest.assertTrue("4 <> 5");
        HTest.assertTrue("4 <= 5");
        HTest.assertFalse("4 > 5");
        HTest.assertFalse("4 >= 5");
    }

    @Test
    public void stringCompares() throws HPersistException {

        HTest.assertTrue("'aaa' == 'aaa'");
        HTest.assertFalse("'aaa' != 'aaa'");
        HTest.assertFalse("'aaa' <> 'aaa'");
        HTest.assertFalse("'aaa' == 'bbb'");
        HTest.assertTrue("'aaa' <= 'bbb'");
        HTest.assertTrue("'bbb' <= 'bbb'");
        HTest.assertFalse("'bbb' <= 'aaa'");
        HTest.assertFalse("'bbb' > 'bbb'");
        HTest.assertTrue("'bbb' > 'aaa'");
        HTest.assertTrue("'bbb' >= 'aaa'");
        HTest.assertTrue("'aaa' >= 'aaa'");
    }

    @Test
    public void numericCalculations() throws HPersistException {

        HTest.assertTrue("9 == 9");
        HTest.assertTrue("((4 + 5) == 9)");
        HTest.assertTrue("(9) == 9");
        HTest.assertTrue("(4 + 5) == 9");
        HTest.assertFalse("(4 + 5) == 8");
        HTest.assertTrue("(4 + 5 + 10 + 10 - 20) == 9");
        HTest.assertFalse("(4 + 5 + 10 + 10 - 20) != 9");
    }

    @Test
    public void numericFunctions() throws HPersistException {

        HTest.assertTrue("3 between 2 AND 5");
        HTest.assertTrue("3 between (1+1) AND (3+2)");

        HTest.assertTrue("3 in (2,3,4)");
        HTest.assertFalse("3 in (1+1,1+3,4)");
        HTest.assertTrue("3 in (1+1,1+2,4)");

    }

    @Test
    public void stringFunctions() throws HPersistException {

        HTest.assertTrue("'bbb' between 'aaa' AND 'ccc'");
        HTest.assertTrue("'bbb' between 'bbb' AND 'ccc'");
        HTest.assertFalse("'bbb' between 'ccc' AND 'ddd'");

        HTest.assertTrue("('bbb' between 'bbb' AND 'ccc') AND ('fff' between 'eee' AND 'ggg')");
    }

    @Test
    public void objectFunctions() throws HPersistException {

        AllTypes obj = new AllTypes("aaa", 1, "bbb");

        HTest.assertTrue("stringValue between 'aaa' AND 'ccc'", obj);
        HTest.assertTrue("stringValue between 'bbb' AND 'ccc'", obj);
        HTest.assertFalse("stringValue between 'ccc' AND 'ddd'", obj);
        HTest.assertTrue("('bbb' between stringValue AND 'ccc') AND ('fff' between 'eee' AND 'ggg')", obj);
    }

}

