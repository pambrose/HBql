package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
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
    public Class<? extends ValueExpr> validateType() throws HPersistException {

        final Class<? extends ValueExpr> type1 = this.getPred().validateType();
        final Class<? extends ValueExpr> type2 = this.getExpr1().validateType();
        final Class<? extends ValueExpr> type3 = this.getExpr2().validateType();

        if (!ExprTree.isOfType(type1, BooleanValue.class))
            throw new HPersistException("Type " + type1.getName() + " not valid in BooleanTernary");

        if (!type2.equals(type3))
            throw new HPersistException("Type mismatch in BooleanTernary");

        if (!ExprTree.isOfType(type2, BooleanValue.class))
            throw new HPersistException("Type " + type2.getName() + " not valid in BooleanTernary");

        return BooleanValue.class;
    }

    @Override
    public ValueExpr getOptimizedValue() throws HPersistException {

        this.setPred((BooleanValue)this.getPred().getOptimizedValue());
        this.setExpr1((BooleanValue)this.getExpr1().getOptimizedValue());
        this.setExpr2((BooleanValue)this.getExpr2().getOptimizedValue());

        return this.isAConstant() ? new BooleanLiteral(this.getValue(null)) : this;
    }

    @Override
    public Boolean getValue(final Object object) throws HPersistException {
        return (Boolean)super.getValue(object);
    }
}