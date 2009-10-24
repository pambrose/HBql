package org.apache.hadoop.hbase.hbql.stmt.expr.instmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.InternalErrorException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.stmt.util.HUtil;

import java.util.List;

public class DelegateInStmt extends GenericInStmt {

    public DelegateInStmt(final GenericValue arg0, final boolean not, final List<GenericValue> inList) {
        super(arg0, not, inList);
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws HBqlException {

        final Class<? extends GenericValue> type = this.getArg(0).validateTypes(this, false);

        final Class<? extends GenericValue> inType = this.determineGenericValueClass(type);

        // Make sure all the types are matched
        for (final GenericValue val : this.getInList())
            this.validateParentClass(inType, val.validateTypes(this, true));

        if (HUtil.isParentClass(StringValue.class, type))
            this.setTypedExpr(new StringInStmt(this.getArg(0), this.isNot(), this.getInList()));
        else if (HUtil.isParentClass(NumberValue.class, type))
            this.setTypedExpr(new NumberInStmt(this.getArg(0), this.isNot(), this.getInList()));
        else if (HUtil.isParentClass(DateValue.class, type))
            this.setTypedExpr(new DateInStmt(this.getArg(0), this.isNot(), this.getInList()));
        else if (HUtil.isParentClass(BooleanValue.class, type))
            this.setTypedExpr(new BooleanInStmt(this.getArg(0), this.isNot(), this.getInList()));
        else
            this.throwInvalidTypeException(type);

        return this.getTypedExpr().validateTypes(parentExpr, false);
    }

    protected boolean evaluateInList(final Object object) throws HBqlException {
        throw new InternalErrorException();
    }

    public GenericValue getOptimizedValue() throws HBqlException {
        this.optimizeArgs();
        return !this.isAConstant() ? this : this.getTypedExpr().getOptimizedValue();
    }

    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return this.getTypedExpr().getValue(object);
    }
}