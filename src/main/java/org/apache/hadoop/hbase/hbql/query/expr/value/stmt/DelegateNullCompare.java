package org.apache.hadoop.hbase.hbql.query.expr.value.stmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.util.HUtil;

public class DelegateNullCompare extends GenericNullCompare {

    public DelegateNullCompare(final boolean not, final GenericValue expr) {
        super(null, not, expr);
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws HBqlException {

        final Class<? extends GenericValue> type = this.determineGenericValueClass(this.getArg(0).validateTypes(this,
                                                                                                                false));

        if (HUtil.isParentClass(StringValue.class, type))
            this.setTypedExpr(new StringNullCompare(this.isNot(), this.getArg(0)));
        else if (HUtil.isParentClass(NumberValue.class, type))
            this.setTypedExpr(new NumberNullCompare(this.isNot(), this.getArg(0)));
        else if (HUtil.isParentClass(DateValue.class, type))
            this.setTypedExpr(new DateNullCompare(this.isNot(), this.getArg(0)));
        else if (HUtil.isParentClass(BooleanValue.class, type))
            this.setTypedExpr(new BooleanNullCompare(this.isNot(), this.getArg(0)));
        else
            this.throwInvalidTypeException(type);

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