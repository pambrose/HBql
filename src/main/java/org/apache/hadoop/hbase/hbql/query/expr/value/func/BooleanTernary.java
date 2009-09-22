package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class BooleanTernary extends GenericTernary<BooleanValue> implements BooleanValue {

    public BooleanTernary(final BooleanValue pred, final BooleanValue expr1, final BooleanValue expr2) {
        super(pred, expr1, expr2);
    }

    @Override
    public ValueExpr getOptimizedValue(final Object object) throws HPersistException {

        this.setPred((BooleanValue)this.getPred().getOptimizedValue(object));
        this.setExpr1((BooleanValue)this.getExpr1().getOptimizedValue(object));
        this.setExpr2((BooleanValue)this.getExpr2().getOptimizedValue(object));

        return this.isAConstant() ? new BooleanLiteral(this.getValue(object)) : this;
    }

    @Override
    public Boolean getValue(final Object object) throws HPersistException {
        return (Boolean)super.getValue(object);
    }
}