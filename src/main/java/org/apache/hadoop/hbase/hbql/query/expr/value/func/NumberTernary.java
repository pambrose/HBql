package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
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
    public Class<? extends ValueExpr> validateType() throws HPersistException {

        final Class<? extends ValueExpr> type1 = this.getPred().validateType();
        final Class<? extends ValueExpr> type2 = this.getExpr1().validateType();
        final Class<? extends ValueExpr> type3 = this.getExpr2().validateType();

        if (!ExprTree.isOfType(type1, BooleanValue.class))
            throw new HPersistException("Type " + type1.getName() + " not valid in NumberTernary");

        if (!type2.equals(type3))
            throw new HPersistException("Types in DateExpr do not match");

        if (!ExprTree.isOfType(type2, NumberValue.class))
            throw new HPersistException("Type " + type2.getName() + " not valid in NumberTernary");

        return NumberValue.class;
    }

    @Override
    public ValueExpr getOptimizedValue() throws HPersistException {

        this.setPred((BooleanValue)this.getPred().getOptimizedValue());
        this.setExpr1((NumberValue)this.getExpr1().getOptimizedValue());
        this.setExpr2((NumberValue)this.getExpr2().getOptimizedValue());

        return this.isAConstant() ? new NumberLiteral(this.getValue(null)) : this;
    }

    @Override
    public Number getValue(final Object object) throws HPersistException {
        return (Number)super.getValue(object);
    }
}
