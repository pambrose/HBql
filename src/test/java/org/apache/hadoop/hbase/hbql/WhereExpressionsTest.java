package org.apache.hadoop.hbase.hbql;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.antlr.HBql;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.util.TestSupport;
import org.junit.Test;

import java.util.Arrays;
import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 27, 2009
 * Time: 2:13:47 PM
 */
public class WhereExpressionsTest extends TestSupport {


    @Test
    public void booleanExpressions() throws HBqlException {
        assertEvalTrue("TRUE");
        assertEvalTrue("TRUE == TRUE");
        assertEvalTrue("TRUE != FALSE");
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
    public void booleanParamExpressions() throws HBqlException {

        ExprTree tree;

        tree = parseExpr(":test");
        tree.setParameter(":test", Boolean.TRUE);
        assertExprTreeEvalTrue(tree);
        tree.setParameter(":test", Boolean.FALSE);
        assertExprTreeEvalFalse(tree);

        tree = parseExpr(":test AND :test");
        tree.setParameter(":test", Boolean.TRUE);
        assertExprTreeEvalTrue(tree);
        tree.setParameter(":test", Boolean.FALSE);
        assertExprTreeEvalFalse(tree);

        tree = parseExpr(":test1 OR :test2");
        tree.setParameter(":test1", Boolean.TRUE);
        tree.setParameter(":test2", Boolean.FALSE);
        assertExprTreeEvalTrue(tree);
        tree.setParameter(":test1", Boolean.FALSE);
        assertExprTreeEvalFalse(tree);

        tree = parseExpr(":test1");
        assertHasException(tree, TypeException.class);

        tree = parseExpr(":b1 == :b2");
        tree.setParameter("b1", Boolean.TRUE);
        tree.setParameter("b2", Boolean.TRUE);
        assertExprTreeEvalTrue(tree);
        tree.setParameter("b2", Boolean.FALSE);
        assertExprTreeEvalFalse(tree);

        tree = parseExpr(":b1 != :b2");
        tree.setParameter("b1", Boolean.TRUE);
        tree.setParameter("b2", Boolean.FALSE);
        assertExprTreeEvalTrue(tree);
        tree.setParameter("b2", Boolean.TRUE);
        assertExprTreeEvalFalse(tree);

        tree = parseExpr("((((:b1 OR :b1 OR :b1))))" + " OR " + "((((:b1 OR :b1 OR :b1))))");
        tree.setParameter("b1", Boolean.FALSE);
        assertExprTreeEvalFalse(tree);

        tree = parseExpr(":b1 OR ((:b1) or :b1) OR :b2");
        tree.setParameter("b1", Boolean.TRUE);
        tree.setParameter("b2", Boolean.FALSE);
        assertExprTreeEvalTrue(tree);
    }

    @Test
    public void numericCompares() throws HBqlException {

        assertEvalTrue("4 < 5");
        assertEvalFalse("4 = 5");
        assertEvalFalse("4 == 5");
        assertEvalTrue("4 != 5");
        assertEvalTrue("4 <> 5");
        assertEvalTrue("4 <= 5");
        assertEvalFalse("4 > 5");
        assertEvalFalse("4 >= 5");
    }

    @Test
    public void numericParamExpressions() throws HBqlException {

        ExprTree tree;

        tree = parseExpr(":val1 < :val2");
        tree.setParameter("val1", 4);
        tree.setParameter("val2", 5);
        assertExprTreeEvalTrue(tree);
        tree.setParameter(":val2", 3);
        assertExprTreeEvalFalse(tree);
    }


    @Test
    public void dateCompares() throws HBqlException {
        assertEvalTrue("NOW() = NOW()");
        assertEvalTrue("NOW() != NOW()-HOUR(1)");
        assertEvalTrue("NOW() > NOW()-DAY(1)");
        assertEvalTrue("NOW()-DAY(1) < NOW()");
        assertEvalTrue("NOW() <= NOW()+DAY(1)");
        assertEvalTrue("NOW()+DAY(1) >= NOW()");
        assertEvalTrue("NOW() < Date('12/21/2020', 'mm/dd/yyyy')");
        assertEvalTrue("NOW() BETWEEN NOW()-DAY(1) AND NOW()+DAY(1)");
        assertEvalTrue("NOW() IN (NOW()-DAY(1), NOW(), NOW()+DAY(1), Date('12/21/2020', 'mm/dd/yyyy'))");
        assertEvalFalse("NOW() IN (NOW()-DAY(1), NOW()+DAY(1), Date('12/21/2020', 'mm/dd/yyyy'))");
        assertEvalTrue("DATE('10/31/94', 'mm/dd/yy') - DAY(1) = DATE('10/30/94', 'mm/dd/yy')");
        assertEvalTrue("DATE('10/31/94', 'mm/dd/yy') - DAY(2) < DATE('10/30/94', 'mm/dd/yy')");
        assertEvalFalse("DATE('10/31/94', 'mm/dd/yy') - DAY(1) < DATE('10/30/94', 'mm/dd/yy')");
        assertEvalTrue("DATE('10/31/1994', 'mm/dd/yyyy') - DAY(1) - MILLI(1) < DATE('10/31/1994', 'mm/dd/yyyy')  - DAY(1) ");
        assertEvalTrue("DATE('10/31/1994', 'mm/dd/yyyy') - DAY(1) - MILLI(1) = DATE('10/30/1994', 'mm/dd/yyyy')  - MILLI(1) ");
    }

    @Test
    public void dateParamCompares() throws HBqlException {
        ExprTree tree;

        tree = parseExpr("NOW() - DAY(1) = :d1");
        tree.setParameter("d1", new Date());
        assertExprTreeEvalFalse(tree);

        tree = parseExpr("NOW() - DAY(1) < :d1");
        tree.setParameter("d1", new Date());
        assertExprTreeEvalTrue(tree);
    }

    @Test
    public void stringCompares() throws HBqlException {

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
    public void stringParamCompares() throws HBqlException {

        ExprTree tree;

        tree = parseExpr("'aaa' = 'a'+:s1");
        tree.setParameter("s1", "aa");
        assertExprTreeEvalTrue(tree);

        tree = parseExpr("'aaa' = 'a'+:s1");
        tree.setParameter("s1", 1);
        assertHasException(tree, TypeException.class);
    }

    @Test
    public void nullCompares() throws HBqlException {

        assertEvalTrue("NULL IS NULL");
        assertEvalFalse("NULL IS NOT NULL");
    }

    @Test
    public void nullParamCompares() throws HBqlException {

        ExprTree tree;

        tree = parseExpr(":a IS NULL");
        tree.setParameter("a", null);
        assertExprTreeEvalTrue(tree);
        tree.setParameter("a", "val");
        assertExprTreeEvalFalse(tree);

        tree = parseExpr(":a IS NOT NULL");
        tree.setParameter("a", "vall");
        assertExprTreeEvalTrue(tree);
        tree.setParameter("a", null);
        assertExprTreeEvalFalse(tree);

        tree.setParameter("a", 1);
        assertHasException(tree, TypeException.class);
    }

    @Test
    public void numericCalculations() throws HBqlException {

        assertEvalTrue("-9 = -9");
        assertEvalFalse("-9 = -8");
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

        assertTrue(HBql.parseNumberValue("1-2-3-4").intValue() == (1 - 2 - 3 - 4));
        assertTrue(HBql.parseNumberValue("(2-2)-2").intValue() == ((2 - 2) - 2));
        assertTrue(HBql.parseNumberValue("2-(2-2)").intValue() == (2 - (2 - 2)));
        assertTrue(HBql.parseNumberValue("2-2-2").intValue() == (2 - 2 - 2));
        assertTrue(HBql.parseNumberValue("2*3-4*(((20/5)))+2-(2-3-4*3)").intValue()
                   == (2 * 3 - 4 * (((20 / 5))) + 2 - (2 - 3 - 4 * 3)));
        assertTrue(HBql.parseNumberValue("2-(-4)").intValue() == (2 - (-4)));
        assertTrue(HBql.parseNumberValue("(7-4)+2").intValue() == ((7 - 4) + 2));
        assertTrue(HBql.parseNumberValue("7-(4+2)").intValue() == (7 - (4 + 2)));

        assertTrue(HBql.parseNumberValue("((((4+3)*(2-1))*(3/1))-((2+5)*(1+1)))").intValue()
                   == (((4 + 3) * (2 - 1)) * (3 / 1)) - ((2 + 5) * (1 + 1)));

        assertTrue(HBql.parseNumberValue("((2+4)*(9-2))").intValue() == ((2 + 4) * (9 - 2)));
        assertTrue(HBql.parseNumberValue("(((4+3)*(2-1))*(3/1))").intValue() == (((4 + 3) * (2 - 1)) * (3 / 1)));
    }

    @Test
    public void numericParamCalculations() throws HBqlException {

        ExprTree tree;

        tree = parseExpr(":a = :b");
        tree.setParameter("a", 8);
        tree.setParameter("b", 8);
        assertExprTreeEvalTrue(tree);

        tree = parseExpr("(-1*:a) = :b");
        tree.setParameter("a", 8);
        tree.setParameter("b", -8);
        assertExprTreeEvalTrue(tree);

        tree = parseExpr("(-1*-1*:a) = :b");
        tree.setParameter("a", 8);
        tree.setParameter("b", 8);
        assertExprTreeEvalTrue(tree);

        tree = parseExpr("(:a + :a + :a + :a - :a) = :b");
        tree.setParameter("a", 5);
        tree.setParameter("b", 15);
        assertExprTreeEvalTrue(tree);

        tree = parseExpr("(:a % :b) = :c");
        tree.setParameter("a", 40);
        tree.setParameter("b", 6);
        tree.setParameter("c", 4);
        assertExprTreeEvalTrue(tree);
    }


    @Test
    public void booleanFunctions() throws HBqlException {

        assertEvalTrue("'abc' CONTAINS 'b'");
        assertEvalFalse("'abc' CONTAINS 'n'");

        assertEvalTrue("'a' IN ('a', 'b')");
        assertEvalFalse("'a' NOT IN ('a', 'b')");
        assertEvalTrue("NOT 'a' NOT IN ('a', 'b')");
        assertEvalFalse("'a' IN ('d', 'b')");
        assertEvalTrue("'a' NOT IN ('d', 'b')");

        final ObjectAllTypes obj = new ObjectAllTypes("aaabbb", 3, "aaab");

        assertEvalTrue(obj, "keyval CONTAINS 'ab'");
        assertExprEvalFalse(obj, "keyval CONTAINS 'ba'");
        assertExprEvalFalse(obj, "'asasas' CONTAINS stringValue");
        assertEvalTrue(obj, "'xxaaabxx' CONTAINS stringValue");
        assertEvalTrue(obj, "keyval CONTAINS stringValue");
        assertEvalTrue(obj, "keyval+'zz' CONTAINS stringValue+'bbz'");
        assertExprEvalFalse(obj, "NOT(keyval+'zz' CONTAINS stringValue+'bbz')");
        assertExprEvalFalse(obj, "NOT ((('asasas' NOT CONTAINS stringValue)))");

        final AnnotatedAllTypes annoObj = new AnnotatedAllTypes("aaabbb", 3, "aaab");

        assertEvalTrue(annoObj, "keyval CONTAINS 'ab'");
        assertExprEvalFalse(annoObj, "keyval CONTAINS 'ba'");
        assertExprEvalFalse(annoObj, "'asasas' CONTAINS stringValue");
        assertEvalTrue(annoObj, "'xxaaabxx' CONTAINS stringValue");
        assertEvalTrue(annoObj, "keyval CONTAINS stringValue");
        assertEvalTrue(annoObj, "keyval+'zz' CONTAINS stringValue+'bbz'");
        assertExprEvalFalse(annoObj, "NOT(keyval+'zz' CONTAINS stringValue+'bbz')");
        assertEvalTrue(obj, "NOT(keyval+'zz' NOT CONTAINS stringValue+'bbz')");

    }

    @Test
    public void booleanParamFunctions() throws HBqlException {

        ExprTree tree;

        tree = parseExpr(":a CONTAINS :b");
        tree.setParameter("a", "abc");
        tree.setParameter("b", "b");
        assertExprTreeEvalTrue(tree);

        tree.setParameter("b", "z");
        assertExprTreeEvalFalse(tree);

        final ObjectAllTypes obj = new ObjectAllTypes("aaabbb", 3, "aaab");

        tree = parseExpr(obj, "keyval CONTAINS :a");
        tree.setParameter("a", "ab");
        assertExprTreeEvalTrue(obj, tree);
        tree.setParameter("a", "ba");
        assertEvalFalse(obj, tree);

        tree = parseExpr(":a IN (:b, :c)");
        tree.setParameter("a", "a");
        tree.setParameter("b", "b");
        tree.setParameter("c", "a");
        assertExprTreeEvalTrue(tree);

        tree = parseExpr(":a IN (:b)");
        tree.setParameter("a", "a");
        tree.setParameter("b", Arrays.asList("a", "b", "c"));
        assertExprTreeEvalTrue(tree);

        // Test for list where scalar is required
        tree.setParameter("a", Arrays.asList("a", "b", "c"));
        assertHasException(tree, TypeException.class);
    }

    @Test
    public void numericFunctions() throws HBqlException {

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

        assertEvalTrue("LENGTH('abc') = 3");
        assertEvalTrue("LENGTH('') = 0");

        assertEvalTrue("INDEXOF('abc', 'b') = 1");
        assertEvalTrue("INDEXOF('abc', 'v') = (-1)");

        assertEvalTrue("REPLACE('abc', 'a', 'bb') = 'bb'+'b'+'c'");
    }

    @Test
    public void stringFunctions() throws HBqlException {

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
    public void stringParamFunctions() throws HBqlException {

        ExprTree tree;

        tree = parseExpr(":a between :b AND :c");
        tree.setParameter("a", "bbb");
        tree.setParameter("b", "aaa");
        tree.setParameter("c", "ccc");
        assertExprTreeEvalTrue(tree);
    }

    @Test
    public void regexFunctions() throws HBqlException {

        final AnnotatedAllTypes obj = new AnnotatedAllTypes("aaa", 3, "aaab");

        assertEvalTrue("'abc' like 'abc'");
        assertEvalFalse("'abc' not like 'abc'");
        assertEvalTrue("'aaaaab' like 'a*b'");
        assertEvalTrue("'aaaaa' + 'b' like 'a*b'");
        assertEvalTrue("('aaaaa' + 'b') like 'a*b'");
        assertEvalTrue(obj, "stringValue + 'b' like 'a*bb'");
        assertEvalTrue(obj, "(((((stringValue + 'b'))))) like 'a*bb'");
    }

    @Test
    public void objectFunctions() throws HBqlException {

        final AnnotatedAllTypes obj = new AnnotatedAllTypes("aaa", 3, "bbb");

        assertEvalTrue(obj, "stringValue between 'aaa' AND 'ccc'");
        assertEvalTrue(obj, "stringValue between 'aaa' AND 'ccc' AND stringValue between 'aaa' AND 'ccc'");
        assertEvalTrue(obj, "stringValue between 'bbb' AND 'ccc'");
        assertExprEvalFalse(obj, "stringValue between 'ccc' AND 'ddd'");
        assertEvalTrue(obj, "('bbb' between stringValue AND 'ccc') AND ('fff' between 'eee' AND 'ggg')");

        assertEvalTrue(obj, "intValue between 2 AND 5");
        assertEvalTrue(obj, "intValue between (1+1) AND (intValue+2)");
        assertExprEvalFalse(obj, "stringValue IN ('v2', 'v0', 'v999')");
        assertEvalTrue(obj, "'v19' = 'v19'");
        assertExprEvalFalse(obj, "'v19'= stringValue");
        assertExprEvalFalse(obj, "stringValue = 'v19'");
        assertExprEvalFalse(obj, "stringValue = 'v19' OR stringValue IN ('v2', 'v0', 'v999')");
        assertEvalTrue(obj, "stringValue IS NOT NULL");
        assertExprEvalFalse(obj, "stringValue IS NULL");
    }

    @Test
    public void columnLookups() throws HBqlException {

        // assertExprColumnsMatchTrue("{fam1:col1 int, fam2:col2 int} TRUE");
        // assertExprColumnsMatchFalse("TRUE", "intValue");
        assertExprColumnsMatchTrue("{intValue  int, int2  integer} intValue between 2 AND 5", "intValue");
        assertInvalidInput("{xintValue  int} xintValue between 2 AND 5", "intValue");
        assertExprColumnsMatchTrue("{a1 date,a2  date} a1 < a2", "a1", "a2");
        assertExprColumnsMatchFalse("{a1 int, a2 int, d1 int, k3 int} a1 < a2 OR d1 > k3", "a1", "a2");
        assertExprColumnsMatchTrue("{a1 date, a2 date, d1 date, k3  date} a1 < a2 OR d1 > k3", "a1", "a2", "d1", "k3");
    }

    @Test
    public void intervalExpressions() throws HBqlException {
        assertEvalTrue("NOW() < NOW()+YEAR(1)");
        assertEvalTrue("NOW() = NOW()+YEAR(1)-YEAR(1)");
        assertEvalTrue("NOW()+YEAR(2) > NOW()+YEAR(1)");

        assertEvalTrue("NOW() < NOW()+WEEK(1)");
        assertEvalTrue("YEAR(2) = WEEK(52*2)");
        assertEvalTrue("NOW()+YEAR(2) = NOW()+WEEK(52*2)");

        assertEvalTrue("NOW()+YEAR(2) = NOW()+WEEK(52)+DAY(364)");

        assertEvalTrue("NOW() BETWEEN NOW()-DAY(1) AND NOW()+DAY(1)");
        assertEvalTrue("NOW() between NOW()-DAY(1) AND NOW()+DAY(1)");
        assertEvalFalse("NOW() BETWEEN NOW()+DAY(1) AND NOW()+DAY(1)");
    }

    @Test
    public void intervalParamExpressions() throws HBqlException {

        ExprTree tree;

        tree = parseExpr("NOW() < NOW()+MINUTE(:a)");
        tree.setParameter("a", 1);
        assertExprTreeEvalTrue(tree);

        tree = parseExpr("NOW()+YEAR(:a) = NOW()+WEEK(:b)+DAY(:c)");
        tree.setParameter("a", 2);
        tree.setParameter("b", 52);
        tree.setParameter("c", 364);
        assertExprTreeEvalTrue(tree);
    }

}

