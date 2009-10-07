package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.util.HUtil;

public class DelegateBetweenStmt extends GenericBetweenStmt {

    private GenericBetweenStmt typedExpr = null;

    public DelegateBetweenStmt(final GenericValue arg0,
                               final boolean not,
                               final GenericValue arg1,
                               final GenericValue arg2) {
        super(null, not, arg0, arg1, arg2);
    }

    private GenericBetweenStmt getTypedExpr() {
        return typedExpr;
    }

    private void setTypedExpr(final GenericBetweenStmt typedExpr) {
        this.typedExpr = typedExpr;
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {

        final Class<? extends GenericValue> type1 = this.getArg(0).validateTypes(this, false);
        final Class<? extends GenericValue> type2 = this.getArg(1).validateTypes(this, false);
        final Class<? extends GenericValue> type3 = this.getArg(2).validateTypes(this, false);

        if (HUtil.isParentClass(StringValue.class, type1, type2, type3))
            this.setTypedExpr(new StringBetweenStmt(this.getArg(0), this.isNot(), this.getArg(1), this.getArg(2)));
        else if (HUtil.isParentClass(NumberValue.class, type1, type2, type3))
            this.setTypedExpr(new NumberBetweenStmt(this.getArg(0), this.isNot(), this.getArg(1), this.getArg(2)));
        else if (HUtil.isParentClass(DateValue.class, type1, type2, type3))
            this.setTypedExpr(new DateBetweenStmt(this.getArg(0), this.isNot(), this.getArg(1), this.getArg(2)));
        else
            this.throwInvalidTypeException(type1, type2, type3);

        return this.getTypedExpr().validateTypes(parentExpr, false);
    }

    public GenericValue getOptimizedValue() throws HBqlException {
        this.optimizeArgs();
        return !this.isAConstant() ? this : this.getTypedExpr().getOptimizedValue();
    }

    public Boolean getValue(final Object object) throws HBqlException {
        return this.getTypedExpr().getValue(object);
    }
}