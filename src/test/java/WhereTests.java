import com.imap4j.hbase.hql.HPersistException;
import com.imap4j.hbase.hql.HUtil;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 27, 2009
 * Time: 2:13:47 PM
 */
public class WhereTests {

    @Test
    public void booleanExpressions() throws HPersistException {

        assertTrue(HUtil.test("TRUE"));
        assertFalse(HUtil.test("FALSE"));
        assertTrue(HUtil.test("TRUE OR TRUE"));
        assertTrue(HUtil.test("TRUE OR TRUE OR TRUE"));
        assertFalse(HUtil.test("FALSE OR FALSE OR FALSE"));
        assertFalse(HUtil.test("(FALSE OR FALSE OR FALSE)"));
        assertFalse(HUtil.test("((((FALSE OR FALSE OR FALSE))))" + " OR " + "((((FALSE OR FALSE OR FALSE))))"));
        assertTrue(HUtil.test("TRUE OR FALSE"));
        assertFalse(HUtil.test("FALSE OR FALSE"));
        assertTrue(HUtil.test("TRUE AND TRUE"));
        assertFalse(HUtil.test("TRUE AND FALSE"));
        assertTrue(HUtil.test("TRUE OR ((true) or true) OR FALSE"));
        assertFalse(HUtil.test("(false AND ((true) OR true)) AND TRUE"));
        assertTrue(HUtil.test("(false AND ((true) OR true)) OR TRUE"));
    }

    @Test
    public void numericExpressions() throws HPersistException {

        assertTrue(HUtil.test("TRUE"));
        assertFalse(HUtil.test("FALSE"));
        assertTrue(HUtil.test("TRUE OR TRUE"));
        assertTrue(HUtil.test("TRUE OR TRUE OR TRUE"));
        assertFalse(HUtil.test("FALSE OR FALSE OR FALSE"));
        assertFalse(HUtil.test("(FALSE OR FALSE OR FALSE)"));
        assertFalse(HUtil.test("((((FALSE OR FALSE OR FALSE))))" + " OR " + "((((FALSE OR FALSE OR FALSE))))"));
        assertTrue(HUtil.test("TRUE OR FALSE"));
        assertFalse(HUtil.test("FALSE OR FALSE"));
        assertTrue(HUtil.test("TRUE AND TRUE"));
        assertFalse(HUtil.test("TRUE AND FALSE"));
        assertTrue(HUtil.test("TRUE OR ((true) or true) OR FALSE"));
        assertFalse(HUtil.test("(false AND ((true) OR true)) AND TRUE"));
        assertTrue(HUtil.test("(false AND ((true) OR true)) OR TRUE"));
    }

}

