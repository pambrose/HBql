package org.apache.expreval.expr.nullcomp;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.node.GenericValue;

public class NumberNullCompare extends GenericNullCompare {

    public NumberNullCompare(final boolean not, final GenericValue arg0) {
        super(ExpressionType.NUMBERNULL, not, arg0);
    }

    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        final Number val = (Number)this.getArg(0).getValue(object);
        final boolean retval = (val == null);
        return (this.isNot()) ? !retval : retval;
    }
}