package org.apache.hadoop.hbase.hbql.stmt.expr.compare;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.util.HUtil;
import org.apache.hadoop.hbase.hbql.stmt.expr.Operator;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.StringValue;

public class DelegateCompare extends GenericCompare {

    private GenericCompare typedExpr = null;

    public DelegateCompare(final GenericValue arg0, final Operator operator, final GenericValue arg1) {
        super(arg0, operator, arg1);
    }

    private GenericCompare getTypedExpr() {
        return this.typedExpr;
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws HBqlException {

        final Class<? extends GenericValue> type0 = this.getArg(0).validateTypes(this, false);
        final Class<? extends GenericValue> type1 = this.getArg(1).validateTypes(this, false);

        if (HUtil.isParentClass(StringValue.class, type0, type1))
            typedExpr = new StringCompare(this.getArg(0), this.getOperator(), this.getArg(1));
        else if (HUtil.isParentClass(NumberValue.class, type0, type1))
            typedExpr = new NumberCompare(this.getArg(0), this.getOperator(), this.getArg(1));
        else if (HUtil.isParentClass(DateValue.class, type0, type1))
            typedExpr = new DateCompare(this.getArg(0), this.getOperator(), this.getArg(1));
        else if (HUtil.isParentClass(BooleanValue.class, type0, type1))
            typedExpr = new BooleanCompare(this.getArg(0), this.getOperator(), this.getArg(1));
        else
            this.throwInvalidTypeException(type0, type1);

        return this.getTypedExpr().validateTypes(parentExpr, false);
    }

    public GenericValue getOptimizedValue() throws HBqlException {
        this.optimizeArgs();
        return !this.isAConstant() ? this : this.getTypedExpr().getOptimizedValue();
    }

    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return this.getTypedExpr().getValue(object);
    }
}