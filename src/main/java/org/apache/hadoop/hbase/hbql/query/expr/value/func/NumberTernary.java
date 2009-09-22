package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.NumberLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class NumberTernary extends GenericTernary<NumberValue> implements NumberValue {

    public NumberTernary(final BooleanValue pred, final NumberValue expr1, final NumberValue expr2) {
        super(pred, expr1, expr2);
    }

    @Override
    public ValueExpr getOptimizedValue(final Object object) throws HPersistException {

        this.setPred((BooleanValue)this.getPred().getOptimizedValue(object));
        this.setExpr1((NumberValue)this.getExpr1().getOptimizedValue(object));
        this.setExpr2((NumberValue)this.getExpr2().getOptimizedValue(object));

        return this.isAConstant() ? new NumberLiteral(this.getValue(object)) : this;
    }

    @Override
    public Number getValue(final Object object) throws HPersistException {
        return (Number)super.getValue(object);
    }
}
