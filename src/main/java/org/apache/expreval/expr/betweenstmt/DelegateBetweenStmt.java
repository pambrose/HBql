package org.apache.expreval.expr.betweenstmt;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.Util;
import org.apache.expreval.expr.node.DateValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.expreval.expr.node.StringValue;

public class DelegateBetweenStmt extends GenericBetweenStmt {

    public DelegateBetweenStmt(final GenericValue arg0,
                               final boolean not,
                               final GenericValue arg1,
                               final GenericValue arg2) {
        super(null, not, arg0, arg1, arg2);
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws HBqlException {

        final Class<? extends GenericValue> type1 = this.getArg(0).validateTypes(this, false);
        final Class<? extends GenericValue> type2 = this.getArg(1).validateTypes(this, false);
        final Class<? extends GenericValue> type3 = this.getArg(2).validateTypes(this, false);

        if (Util.isParentClass(StringValue.class, type1, type2, type3))
            this.setTypedExpr(new StringBetweenStmt(this.getArg(0), this.isNot(), this.getArg(1), this.getArg(2)));
        else if (Util.isParentClass(NumberValue.class, type1, type2, type3))
            this.setTypedExpr(new NumberBetweenStmt(this.getArg(0), this.isNot(), this.getArg(1), this.getArg(2)));
        else if (Util.isParentClass(DateValue.class, type1, type2, type3))
            this.setTypedExpr(new DateBetweenStmt(this.getArg(0), this.isNot(), this.getArg(1), this.getArg(2)));
        else
            this.throwInvalidTypeException(type1, type2, type3);

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