package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.DateLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class DateTernary extends GenericTernary implements DateValue {

    public DateTernary(final ValueExpr pred, final ValueExpr expr1, final ValueExpr expr2) {
        super(pred, expr1, expr2);
    }

    @Override
    public Class<? extends ValueExpr> validateType() throws HBqlException {
        return this.validateType(DateValue.class, "DateTernary");
    }

    @Override
    public ValueExpr getOptimizedValue() throws HBqlException {

        this.setPred(this.getPred().getOptimizedValue());
        this.setExpr1(this.getExpr1().getOptimizedValue());
        this.setExpr2(this.getExpr2().getOptimizedValue());

        return this.isAConstant() ? new DateLiteral(this.getValue(null)) : this;
    }

    @Override
    public Long getValue(final Object object) throws HBqlException {
        return (Long)super.getValue(object);
    }
}