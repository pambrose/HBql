package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class DelegateCalculation extends GenericCalculation {

    private GenericCalculation typedExpr = null;

    public DelegateCalculation(final ValueExpr expr1, final Operator operator, final ValueExpr expr2) {
        super(expr1, operator, expr2);
    }

    @Override
    public Class<? extends ValueExpr> validateTypes(final ValueExpr parentExpr,
                                                    final boolean allowsCollections) throws TypeException {

        final Class<? extends ValueExpr> type1 = this.getExpr1().validateTypes(this, false);
        final Class<? extends ValueExpr> type2 = this.getExpr2().validateTypes(this, false);

        if (HUtil.isParentClass(StringValue.class, type1, type2))
            typedExpr = new StringCalculation(this.getExpr1(), this.getOperator(), this.getExpr2());
        else if (HUtil.isParentClass(NumberValue.class, type1, type2))
            typedExpr = new NumberCalculation(this.getExpr1(), this.getOperator(), this.getExpr2());
        else if (HUtil.isParentClass(DateValue.class, type1, type2))
            typedExpr = new DateCalculation(this.getExpr1(), this.getOperator(), this.getExpr2());
        else
            HUtil.throwInvalidTypeException(this, type1, type2);

        return this.typedExpr.validateTypes(parentExpr, false);
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