package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.DateLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class DateTernary extends GenericTernary<DateValue> implements DateValue {


    public DateTernary(final BooleanValue pred, final DateValue expr1, final DateValue expr2) {
        super(pred, expr1, expr2);
    }

    @Override
    public ValueExpr getOptimizedValue() throws HPersistException {

        this.setPred((BooleanValue)this.getPred().getOptimizedValue());
        this.setExpr1((DateValue)this.getExpr1().getOptimizedValue());
        this.setExpr2((DateValue)this.getExpr2().getOptimizedValue());

        return this.isAConstant() ? new DateLiteral(this.getValue(null)) : this;
    }

    @Override
    public Long getValue(final Object object) throws HPersistException {
        return (Long)super.getValue(object);
    }
}