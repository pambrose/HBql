package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class StringTernary extends GenericTernary<StringValue> implements StringValue {

    public StringTernary(final BooleanValue pred, final StringValue expr1, final StringValue expr2) {
        super(pred, expr1, expr2);
    }

    @Override
    public Class<? extends ValueExpr> validateType() throws HPersistException {

        final Class<? extends ValueExpr> type1 = this.getPred().validateType();
        final Class<? extends ValueExpr> type2 = this.getExpr1().validateType();
        final Class<? extends ValueExpr> type3 = this.getExpr2().validateType();

        if (!ExprTree.isOfType(type1, BooleanValue.class))
            throw new HPersistException("Type " + type1.getName() + " not valid in StringTernary");

        if (!type2.equals(type3))
            throw new HPersistException("Type mismatch in StringTernary");

        if (!ExprTree.isOfType(type2, StringValue.class))
            throw new HPersistException("Type " + type2.getName() + " not valid in StringTernary");

        return StringValue.class;
    }

    @Override
    public ValueExpr getOptimizedValue() throws HPersistException {

        this.setPred((BooleanValue)this.getPred().getOptimizedValue());
        this.setExpr1((StringValue)this.getExpr1().getOptimizedValue());
        this.setExpr2((StringValue)this.getExpr2().getOptimizedValue());

        return this.isAConstant() ? new BooleanLiteral(this.getValue(null)) : this;
    }

    @Override
    public String getValue(final Object object) throws HPersistException {
        return (String)super.getValue(object);
    }
}