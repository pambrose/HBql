package org.apache.hadoop.hbase.hbql;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.util.WhereExprTests;
import org.junit.Test;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 27, 2009
 * Time: 2:13:47 PM
 */
public class IntervalValuesTest extends WhereExprTests {

    @Test
    public void keysExpressions() throws HBqlException {
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
    public void keysParamExpressions() throws HBqlException {

        ExprTree tree;

        tree = parseExpr("NOW() < NOW()+MINUTE(:a)");
        tree.setParameter("a", 1);
        assertEvalTrue(tree);

        tree = parseExpr("NOW()+YEAR(:a) = NOW()+WEEK(:b)+DAY(:c)");
        tree.setParameter("a", 2);
        tree.setParameter("b", 52);
        tree.setParameter("c", 364);
        assertEvalTrue(tree);
    }

}