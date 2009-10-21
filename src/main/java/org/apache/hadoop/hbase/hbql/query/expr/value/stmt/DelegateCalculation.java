package org.apache.hadoop.hbase.hbql.query.expr.value.stmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.util.HUtil;

public class DelegateCalculation extends GenericCalculation {

    public DelegateCalculation(final GenericValue arg0, final Operator operator, final GenericValue arg1) {
        super(null, arg0, operator, arg1);
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {

        final Class<? extends GenericValue> type1 = this.getArg(0).validateTypes(this, false);
        final Class<? extends GenericValue> type2 = this.getArg(1).validateTypes(this, false);

        if (HUtil.isParentClass(StringValue.class, type1, type2))
            this.setTypedExpr(new StringCalculation(this.getArg(0), this.getOperator(), this.getArg(1)));
        else if (HUtil.isParentClass(NumberValue.class, type1, type2))
            this.setTypedExpr(new NumberCalculation(this.getArg(0), this.getOperator(), this.getArg(1)));
        else if (HUtil.isParentClass(DateValue.class, type1, type2))
            this.setTypedExpr(new DateCalculation(this.getArg(0), this.getOperator(), this.getArg(1)));
        else
            this.throwInvalidTypeException(type1, type2);

        return this.getTypedExpr().validateTypes(parentExpr, false);
    }

    public GenericValue getOptimizedValue() throws HBqlException {
        this.optimizeArgs();
        return !this.isAConstant() ? this : this.getTypedExpr().getOptimizedValue();
    }

    public Object getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return this.getTypedExpr().getValue(object);
    }
}