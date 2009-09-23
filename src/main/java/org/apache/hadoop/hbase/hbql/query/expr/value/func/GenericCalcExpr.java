package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.GenericTwoExprExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 7, 2009
 * Time: 9:29:44 PM
 */
public abstract class GenericCalcExpr<T extends ValueExpr> extends GenericTwoExprExpr<T> implements ValueExpr {

    private final Operator op;

    public GenericCalcExpr(final T expr1, final Operator op, final T expr2) {
        super(expr1, expr2);
        this.op = op;
    }

    protected Operator getOp() {
        return op;
    }

    @Override
    public Class<? extends ValueExpr> validateType() throws HPersistException {
        return null;
    }
}
