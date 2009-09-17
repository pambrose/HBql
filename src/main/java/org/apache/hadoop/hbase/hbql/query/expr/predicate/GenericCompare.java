package org.apache.hadoop.hbase.hbql.query.expr.predicate;

import org.apache.hadoop.hbase.hbql.query.expr.node.PredicateExpr;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.GenericTwoExprExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public abstract class GenericCompare<T extends ValueExpr> extends GenericTwoExprExpr<T> implements PredicateExpr {

    public enum OP {
        EQ,
        GT,
        GTEQ,
        LT,
        LTEQ,
        NOTEQ
    }

    private final OP op;

    protected GenericCompare(final T expr1, final OP op, final T expr2) {
        super(expr1, expr2);
        this.op = op;
    }

    protected OP getOp() {
        return op;
    }

}