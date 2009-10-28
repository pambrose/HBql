package org.apache.expreval.expr.instmt;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.util.HUtil;

import java.util.Collection;
import java.util.List;

public class StringInStmt extends GenericInStmt {

    public StringInStmt(final GenericValue arg0, final boolean not, final List<GenericValue> inList) {
        super(arg0, not, inList);
    }

    protected boolean evaluateInList(final Object object) throws HBqlException, ResultMissingColumnException {

        final String attribVal = (String)this.getArg(0).getValue(object);

        for (final GenericValue obj : this.getInList()) {

            // Check if the value returned is a collection
            final Object objval = obj.getValue(object);
            if (HUtil.isACollection(objval)) {
                for (final GenericValue val : (Collection<GenericValue>)objval) {
                    if (attribVal.equals(val.getValue(object)))
                        return true;
                }
            }
            else {
                if (attribVal.equals(objval))
                    return true;
            }
        }
        return false;
    }
}