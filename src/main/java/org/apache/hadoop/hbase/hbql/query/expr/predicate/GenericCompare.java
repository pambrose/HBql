package org.apache.hadoop.hbase.hbql.query.expr.predicate;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.GenericTwoExprExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.func.Operator;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public abstract class GenericCompare<T extends ValueExpr> extends GenericTwoExprExpr<T> implements BooleanValue {

    private final Operator op;

    protected GenericCompare(final T expr1, final Operator op, final T expr2) {
        super(expr1, expr2);
        this.op = op;
    }

    protected Operator getOp() {
        return op;
    }

    @Override
    public ValueExpr getOptimizedValue() throws HPersistException {
        this.setExpr1((T)this.getExpr1().getOptimizedValue());
        this.setExpr2((T)this.getExpr2().getOptimizedValue());

        return this.isAConstant() ? new BooleanLiteral(this.getValue(null)) : this;
    }


    @Override
    public Class<? extends ValueExpr> validateType() throws HPersistException {
        // This is not executed
        return null;
    }

}