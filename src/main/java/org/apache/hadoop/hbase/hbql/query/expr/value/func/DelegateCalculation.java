package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class DelegateCalculation extends GenericCalculation {

    private GenericCalculation typedExpr = null;

    public DelegateCalculation(final GenericValue arg0, final Operator operator, final GenericValue arg1) {
        super(arg0, operator, arg1);
    }

    @Override
    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {

        final Class<? extends GenericValue> type1 = this.getArg(0).validateTypes(this, false);
        final Class<? extends GenericValue> type2 = this.getArg(1).validateTypes(this, false);

        if (HUtil.isParentClass(StringValue.class, type1, type2))
            typedExpr = new StringCalculation(this.getArg(0), this.getOperator(), this.getArg(1));
        else if (HUtil.isParentClass(NumberValue.class, type1, type2))
            typedExpr = new NumberCalculation(this.getArg(0), this.getOperator(), this.getArg(1));
        else if (HUtil.isParentClass(DateValue.class, type1, type2))
            typedExpr = new DateCalculation(this.getArg(0), this.getOperator(), this.getArg(1));
        else
            HUtil.throwInvalidTypeException(this, type1, type2);

        return this.typedExpr.validateTypes(parentExpr, false);
    }

    @Override
    public GenericValue getOptimizedValue() throws HBqlException {
        return this.typedExpr.getOptimizedValue();
    }

    @Override
    public Object getValue(final Object object) throws HBqlException {
        return this.typedExpr.getValue(object);
    }
}