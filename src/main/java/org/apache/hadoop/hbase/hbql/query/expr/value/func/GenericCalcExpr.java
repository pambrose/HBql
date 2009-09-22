package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.GenericTwoExprExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 7, 2009
 * Time: 9:29:44 PM
 */
public class GenericCalcExpr<T extends ValueExpr> extends GenericTwoExprExpr<T> {

    public enum OP {
        PLUS,
        MINUS,
        MULT,
        DIV,
        MOD,
        NEGATIVE
    }

    private final GenericCalcExpr.OP op;

    public GenericCalcExpr(final T expr1, final OP op, final T expr2) {
        super(expr1, expr2);
        this.op = op;
    }

    protected OP getOp() {
        return op;
    }

}
