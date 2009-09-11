import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.schema.HUtil;
import org.junit.Test;
import util.WhereExprTests;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 27, 2009
 * Time: 2:13:47 PM
 */
public class TestWhereExpression extends WhereExprTests {


    @Test
    public void booleanExpressions() throws HPersistException {
        assertEvalTrue("TRUE");
        assertEvalFalse("NOT TRUE");
        assertEvalFalse("NOT TRUE");
        assertEvalFalse("NOT TRUE");
        assertEvalFalse("NOT(((((TRUE)))))");
        assertEvalTrue("((TRUE))");
        assertEvalTrue("(((((TRUE)))))");
        assertEvalFalse("NOT((NOT(((NOT TRUE)))))");
        assertEvalFalse("FALSE");
        assertEvalTrue("TRUE OR TRUE");
        assertEvalTrue("TRUE OR TRUE OR TRUE");
        assertEvalFalse("FALSE OR FALSE OR FALSE");
        assertEvalFalse("(FALSE OR FALSE OR FALSE)");
        assertEvalFalse("((((FALSE OR FALSE OR FALSE))))" + " OR " + "((((FALSE OR FALSE OR FALSE))))");
        assertEvalTrue("TRUE OR FALSE");
        assertEvalFalse("FALSE OR FALSE");
        assertEvalTrue("TRUE AND TRUE");
        assertEvalFalse("TRUE AND FALSE");
        assertEvalTrue("TRUE OR ((true) or true) OR FALSE");
        assertEvalFalse("(false AND ((true) OR true)) AND TRUE");
        assertEvalTrue("(false AND ((true) OR true)) OR TRUE");
    }

    @Test
    public void numericCompares() throws HPersistException {

        assertEvalTrue("4 < 5");
        assertEvalFalse("4 = 5");
        assertEvalFalse("4 = 5");
        assertEvalTrue("4 != 5");
        assertEvalTrue("4 <> 5");
        assertEvalTrue("4 <= 5");
        assertEvalFalse("4 > 5");
        assertEvalFalse("4 >= 5");
    }

    @Test
    public void dateCompares() throws HPersistException {
        assertEvalTrue("NOW() = NOW()");
        assertEvalTrue("NOW() != NOW()-HOUR(1)");
        assertEvalTrue("NOW() > NOW()-DAY(1)");
        assertEvalTrue("NOW()-DAY(1) < NOW()");
        assertEvalTrue("NOW() <= NOW()+DAY(1)");
        assertEvalTrue("NOW()+DAY(1) >= NOW()");
        assertEvalTrue("NOW() < Date('mm/dd/yyyy', '12/21/2020')");
        assertEvalTrue("NOW() BETWEEN NOW()-DAY(1) AND NOW()+DAY(1)");
        assertEvalTrue("NOW() IN (NOW()-DAY(1), NOW(), NOW()+DAY(1), Date('mm/dd/yyyy', '12/21/2020'))");
        assertEvalFalse("NOW() IN (NOW()-DAY(1), NOW()+DAY(1), Date('mm/dd/yyyy', '12/21/2020'))");
        assertEvalTrue("DATE('mm/dd/yy', '10/31/94') - DAY(1) = DATE('mm/dd/yy', '10/30/94')");
        assertEvalTrue("DATE('mm/dd/yy', '10/31/94') - DAY(2) < DATE('mm/dd/yy', '10/30/94')");
        assertEvalFalse("DATE('mm/dd/yy', '10/31/94') - DAY(1) < DATE('mm/dd/yy', '10/30/94')");
        assertEvalTrue("DATE('mm/dd/yyyy', '10/31/1994') - DAY(1) - MILLI(1) < DATE('mm/dd/yyyy', '10/31/1994')  - DAY(1) ");
        assertEvalTrue("DATE('mm/dd/yyyy', '10/31/1994') - DAY(1) - MILLI(1) = DATE('mm/dd/yyyy', '10/30/1994')  - MILLI(1) ");

    }

    @Test
    public void stringCompares() throws HPersistException {

        assertEvalTrue("'aaa' = 'aaa'");
        assertEvalFalse("'aaa' != 'aaa'");
        assertEvalFalse("'aaa' <> 'aaa'");
        assertEvalFalse("'aaa' = 'bbb'");
        assertEvalTrue("'aaa' <= 'bbb'");
        assertEvalTrue("'bbb' <= 'bbb'");
        assertEvalFalse("'bbb' <= 'aaa'");
        assertEvalFalse("'bbb' > 'bbb'");
        assertEvalTrue("'bbb' > 'aaa'");
        assertEvalTrue("'bbb' >= 'aaa'");
        assertEvalTrue("'aaa' >= 'aaa'");
    }

    @Test
    public void nullCompares() throws HPersistException {

        assertEvalTrue("NULL IS NULL");
        assertEvalFalse("NULL IS NOT NULL");
    }

    @Test
    public void numericCalculations() throws HPersistException {

        assertEvalTrue("9 = 9");
        assertEvalTrue("((4 + 5) = 9)");
        assertEvalTrue("(9) = 9");
        assertEvalTrue("(4 + 5) = 9");
        assertEvalFalse("(4 + 5) = 8");
        assertEvalTrue("(4 + 5 + 10 + 10 - 20) = 9");
        assertEvalFalse("(4 + 5 + 10 + 10 - 20) != 9");

        assertEvalTrue("(4 * 5) = 20");
        assertEvalTrue("(40 % 6) = 4");
        assertEvalFalse("(40 % 6) = 3");

        assertTrue(HUtil.parseNumericExpr("1-2-3-4").intValue() == (1 - 2 - 3 - 4));
        assertTrue(HUtil.parseNumericExpr("(2-2)-2").intValue() == ((2 - 2) - 2));
        assertTrue(HUtil.parseNumericExpr("2-(2-2)").intValue() == (2 - (2 - 2)));
        assertTrue(HUtil.parseNumericExpr("2-2-2").intValue() == (2 - 2 - 2));
        assertTrue(HUtil.parseNumericExpr("2*3-4*(((20/5)))+2-(2-3-4*3)").intValue()
                   == (2 * 3 - 4 * (((20 / 5))) + 2 - (2 - 3 - 4 * 3)));
        assertTrue(HUtil.parseNumericExpr("2-(-4)").intValue() == (2 - (-4)));
        assertTrue(HUtil.parseNumericExpr("(7-4)+2").intValue() == ((7 - 4) + 2));
        assertTrue(HUtil.parseNumericExpr("7-(4+2)").intValue() == (7 - (4 + 2)));

        assertTrue(HUtil.parseNumericExpr("((((4+3)*(2-1))*(3/1))-((2+5)*(1+1)))").intValue()
                   == (((4 + 3) * (2 - 1)) * (3 / 1)) - ((2 + 5) * (1 + 1)));

        assertTrue(HUtil.parseNumericExpr("((2+4)*(9-2))").intValue() == ((2 + 4) * (9 - 2)));
        assertTrue(HUtil.parseNumericExpr("(((4+3)*(2-1))*(3/1))").intValue() == (((4 + 3) * (2 - 1)) * (3 / 1)));
    }

    @Test
    public void numericFunctions() throws HPersistException {

        assertEvalTrue("3 between 2 AND 5");
        assertEvalTrue("3 between (1+1) AND (3+2)");
        assertEvalTrue("3 between (1+1) AND (3+2)");

        assertEvalTrue("3 in (2,3,4)");
        assertEvalFalse("3 in (1+1,1+3,4)");
        assertEvalTrue("3 in (1+1,1+2,4)");
        assertEvalFalse("3 NOT in (1+1,1+2,4)");
        assertEvalFalse("3 NOT in (1+1,1+2,4)");
        assertEvalTrue("3 = IF true THEN 3 ELSE 2 END");
        assertEvalFalse("3 = IF false THEN 3 else 2 END");
        assertEvalTrue("2 = IF false THEN 3 else 2 END");

    }

    @Test
    public void stringFunctions() throws HPersistException {

        assertEvalTrue("'bbb' between 'aaa' AND 'ccc'");
        assertEvalTrue("'bbb' between 'aaa' AND 'ccc'");
        assertEvalTrue("'bbb' between 'bbb' AND 'ccc'");
        assertEvalFalse("'bbb' between 'ccc' AND 'ddd'");
        assertEvalTrue("('bbb' between 'bbb' AND 'ccc') AND ('fff' between 'eee' AND 'ggg')");
        assertEvalTrue("('bbb' between 'bbb' AND 'ccc') OR ('fff' between 'eee' AND 'ggg')");
        assertEvalFalse("('bbb' not between 'bbb' AND 'ccc') AND ('fff' between 'eee' AND 'ggg')");
        assertEvalTrue("'bbb' = LOWER('BBB')");
        assertEvalTrue("'ABABAB' = UPPER(CONCAT('aba', 'bab'))");
        assertEvalTrue("'bbb' = SUBSTRING('BBBbbbAAA', 3, 6)");
        assertEvalTrue("'AAA' = 'A' + 'A' + 'A'");
        assertEvalTrue("'aaa' = LOWER('A' + 'A' + 'A')");
    }

    @Test
    public void regexFunctions() throws HPersistException {

        final AllTypes obj = new AllTypes("aaa", 3, "aaab");

        assertEvalTrue("'abc' like 'abc'");
        assertEvalFalse("'abc' not like 'abc'");
        assertEvalTrue("'aaaaab' like 'a*b'");
        assertEvalTrue("'aaaaa' + 'b' like 'a*b'");
        assertEvalTrue("('aaaaa' + 'b') like 'a*b'");
        assertEvalTrue(obj, "stringValue + 'b' like 'a*bb'");
        assertEvalTrue(obj, "(((((stringValue + 'b'))))) like 'a*bb'");
    }

    @Test
    public void objectFunctions() throws HPersistException {

        final AllTypes obj = new AllTypes("aaa", 3, "bbb");

        assertEvalTrue(obj, "stringValue between 'aaa' AND 'ccc'");
        assertEvalTrue(obj, "stringValue between 'aaa' AND 'ccc' AND stringValue between 'aaa' AND 'ccc'");
        assertEvalTrue(obj, "stringValue between 'bbb' AND 'ccc'");
        assertEvalFalse(obj, "stringValue between 'ccc' AND 'ddd'");
        assertEvalTrue(obj, "('bbb' between stringValue AND 'ccc') AND ('fff' between 'eee' AND 'ggg')");

        assertEvalTrue(obj, "intValue between 2 AND 5");
        assertEvalTrue(obj, "intValue between (1+1) AND (intValue+2)");
        assertEvalFalse(obj, "stringValue IN ('v2', 'v0', 'v999')");
        assertEvalTrue(obj, "'v19' = 'v19'");
        assertEvalFalse(obj, "'v19'= stringValue");
        assertEvalFalse(obj, "stringValue = 'v19'");
        assertEvalFalse(obj, "stringValue = 'v19' OR stringValue IN ('v2', 'v0', 'v999')");
        assertEvalTrue(obj, "stringValue IS NOT NULL");
        assertEvalFalse(obj, "stringValue IS NULL");
    }

    @Test
    public void columnLookups() throws HPersistException {
        assertInvalidInput("{a1, a2 as date} a1 < a2");
        assertColumnsMatchTrue("{fam1:col1, fam2:col2 as int} TRUE");
        assertColumnsMatchFalse("TRUE", "intValue");
        assertColumnsMatchTrue("{intValue as int, int2 as integer} intValue between 2 AND 5", "intValue");
        assertInvalidInput("{xintValue as int} xintValue between 2 AND 5", "intValue");
        assertColumnsMatchTrue("{a1,a2 as date} a1 < a2", "a1", "a2");
        assertColumnsMatchFalse("{a1, a2, d1, k3 as int} a1 < a2 OR d1 > k3", "a1", "a2");
        assertColumnsMatchTrue("{a1, a2, d1 as date, k3 as date} a1 < a2 OR d1 > k3", "a1", "a2", "d1", "k3");
    }

}

