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

public class DelegateCaseWhen extends GenericCaseWhen {

    private GenericCaseWhen typedExpr = null;

    public DelegateCaseWhen(final GenericValue arg0, final GenericValue arg1) {
        super(null, arg0, arg1);
    }

    private GenericCaseWhen getTypedExpr() {
        return typedExpr;
    }

    private void setTypedExpr(final GenericCaseWhen typedExpr) {
        this.typedExpr = typedExpr;
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {

        this.validateParentClass(BooleanValue.class, this.getArg(0).validateTypes(this, false));

        final Class<? extends GenericValue> valueType = this.getArg(1).validateTypes(this, false);

        if (HUtil.isParentClass(StringValue.class, valueType))
            this.setTypedExpr(new StringCaseWhen(this.getArg(0), this.getArg(1)));
        else if (HUtil.isParentClass(NumberValue.class, valueType))
            this.setTypedExpr(new NumberCaseWhen(this.getArg(0), this.getArg(1)));
        else if (HUtil.isParentClass(DateValue.class, valueType))
            this.setTypedExpr(new DateCaseWhen(this.getArg(0), this.getArg(1)));
        else if (HUtil.isParentClass(BooleanValue.class, valueType))
            this.setTypedExpr(new BooleanCaseWhen(this.getArg(0), this.getArg(1)));
        else
            this.throwInvalidTypeException(valueType);

        return this.getTypedExpr().validateTypes(parentExpr, false);
    }

    public GenericValue getOptimizedValue() throws HBqlException {
        this.optimizeArgs();
        return this;
    }

    public Object getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return this.getTypedExpr().getValue(object);
    }
}