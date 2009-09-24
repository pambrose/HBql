package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

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

        final Class<? extends ValueExpr> pred = this.getPred().validateType();
        final Class<? extends ValueExpr> type1 = this.getExpr1().validateType();
        final Class<? extends ValueExpr> type2 = this.getExpr2().validateType();

        if (!HUtil.isParentClass(BooleanValue.class, pred))
            throw new HPersistException("Invalid type " + pred.getName() + " in ValueTernary");

        if (HUtil.isParentClass(StringValue.class, type1, type2)) {
            this.typedExpr = new StringTernary(this.getPred(), this.getExpr1(), this.getExpr2());
            return type1;
        }

        if (HUtil.isParentClass(NumberValue.class, type1, type2)) {
            this.typedExpr = new NumberTernary(this.getPred(), this.getExpr1(), this.getExpr2());
            return type1;
        }

        if (HUtil.isParentClass(DateValue.class, type1, type2)) {
            this.typedExpr = new DateTernary(this.getPred(), this.getExpr1(), this.getExpr2());
            return type1;
        }

        if (HUtil.isParentClass(BooleanValue.class, type1, type2)) {
            this.typedExpr = new BooleanTernary(this.getPred(), this.getExpr1(), this.getExpr2());
            return type1;
        }

        throw new HPersistException("Invalid type " + type1.getName() + " in ValueTernary");
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