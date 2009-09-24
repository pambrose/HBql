package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class ValueTernary extends GenericTernary {

    private GenericTernary typedExpr = null;

    public ValueTernary(final ValueExpr pred, final ValueExpr expr1, final ValueExpr expr2) {
        super(pred, expr1, expr2);
    }

    @Override
    public Class<? extends ValueExpr> validateType() throws HPersistException {

        final Class<? extends ValueExpr> type1 = this.getPred().validateType();
        final Class<? extends ValueExpr> type2 = this.getExpr1().validateType();
        final Class<? extends ValueExpr> type3 = this.getExpr2().validateType();

        if (!ExprTree.isOfType(type1, BooleanValue.class))
            throw new HPersistException("Invalid type " + type1.getName() + " in ValueTernary");

        if (!type2.equals(type3))
            throw new HPersistException("Type mismatch in ValueTernary");

        if (type2.equals(DateValue.class)) {
            this.typedExpr = new DateTernary(this.getPred(), this.getExpr1(), this.getExpr2());
            return type2;
        }

        if (type2.equals(BooleanValue.class)) {
            this.typedExpr = new BooleanTernary(this.getPred(), this.getExpr1(), this.getExpr2());
            return type2;
        }

        if (type2.equals(StringValue.class)) {
            this.typedExpr = new StringTernary(this.getPred(), this.getExpr1(), this.getExpr2());
            return type2;
        }

        if (type2.equals(NumberValue.class)) {
            this.typedExpr = new NumberTernary(this.getPred(), this.getExpr1(), this.getExpr2());
            return type2;
        }

        throw new HPersistException("Invalid type " + type2.getName() + " in ValueTernary");
    }


    @Override
    public ValueExpr getOptimizedValue() throws HPersistException {
        return this.typedExpr.getOptimizedValue();
    }

    @Override
    public Object getValue(final Object object) throws HPersistException {
        return this.typedExpr.getValue(object);
    }

}