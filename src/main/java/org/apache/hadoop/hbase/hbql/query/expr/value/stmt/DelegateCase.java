package org.apache.hadoop.hbase.hbql.query.expr.value.stmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.util.HUtil;

import java.util.ArrayList;

public class DelegateCase extends GenericCase {

    private GenericCase typedExpr = null;

    public DelegateCase() {
        super(null, new ArrayList<GenericCaseWhen>(), null);
    }

    private GenericCase getTypedExpr() {
        return typedExpr;
    }

    private void setTypedExpr(final GenericCase typedExpr) {
        this.typedExpr = typedExpr;
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {

        final Class<? extends GenericValue> type = this.getWhenExprList().get(0).validateTypes(this, false);
        final Class<? extends GenericValue> argType = this.determineGenericValueClass(type);

        for (final GenericCaseWhen val : this.getWhenExprList())
            this.validateParentClass(argType, val.validateTypes(this, false));

        if (this.getElseExpr() != null)
            this.validateParentClass(argType, this.getElseExpr().validateTypes(parentExpr, false));

        if (HUtil.isParentClass(StringValue.class, argType))
            this.setTypedExpr(new StringCase(this.getWhenExprList(), this.getElseExpr()));
        else if (HUtil.isParentClass(NumberValue.class, argType))
            this.setTypedExpr(new NumberCase(this.getWhenExprList(), this.getElseExpr()));
        else if (HUtil.isParentClass(DateValue.class, argType))
            this.setTypedExpr(new DateCase(this.getWhenExprList(), this.getElseExpr()));
        else if (HUtil.isParentClass(BooleanValue.class, argType))
            this.setTypedExpr(new BooleanCase(this.getWhenExprList(), this.getElseExpr()));
        else
            this.throwInvalidTypeException(argType);

        return this.getTypedExpr().validateTypes(parentExpr, false);
    }

    public GenericValue getOptimizedValue() throws HBqlException {
        this.optimizeArgs();
        return !this.isAConstant() ? this : this.getTypedExpr().getOptimizedValue();
    }

    public Object getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return this.getTypedExpr().getValue(object);
    }

    public void addWhen(final GenericValue pred, final GenericValue value) {
        this.getWhenExprList().add(new DelegateCaseWhen(pred, value));
    }

    public void addElse(final GenericValue value) {
        if (value != null)
            this.setElseExpr(new DelegateCaseElse(value));
    }
}