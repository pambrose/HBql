package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
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
public class DelegateTernary extends GenericTernary {

    private GenericTernary typedExpr = null;

    public DelegateTernary(final ValueExpr pred, final ValueExpr expr1, final ValueExpr expr2) {
        super(pred, expr1, expr2);
    }

    @Override
    public Class<? extends ValueExpr> validateTypes(final ValueExpr parentExpr,
                                                    final boolean allowsCollections) throws TypeException {

        HUtil.validateParentClass(this, BooleanValue.class, this.getPred().validateTypes(this, false));

        final Class<? extends ValueExpr> type1 = this.getExpr1().validateTypes(this, false);
        final Class<? extends ValueExpr> type2 = this.getExpr2().validateTypes(this, false);

        if (HUtil.isParentClass(StringValue.class, type1, type2))
            this.typedExpr = new StringTernary(this.getPred(), this.getExpr1(), this.getExpr2());
        else if (HUtil.isParentClass(NumberValue.class, type1, type2))
            this.typedExpr = new NumberTernary(this.getPred(), this.getExpr1(), this.getExpr2());
        else if (HUtil.isParentClass(DateValue.class, type1, type2))
            this.typedExpr = new DateTernary(this.getPred(), this.getExpr1(), this.getExpr2());
        else if (HUtil.isParentClass(BooleanValue.class, type1, type2))
            this.typedExpr = new BooleanTernary(this.getPred(), this.getExpr1(), this.getExpr2());
        else
            HUtil.throwInvalidTypeException(this, type1, type2);

        this.typedExpr.validateTypes(parentExpr, false);

        return type1;
    }


    @Override
    public ValueExpr getOptimizedValue() throws HBqlException {
        return this.typedExpr.getOptimizedValue();
    }

    @Override
    public Object getValue(final Object object) throws HBqlException {
        return this.typedExpr.getValue(object);
    }

}