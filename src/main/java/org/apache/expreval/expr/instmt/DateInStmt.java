package org.apache.expreval.expr.instmt;

import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.util.HUtil;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.ResultMissingColumnException;

import java.util.Collection;
import java.util.List;

public class DateInStmt extends GenericInStmt {

    public DateInStmt(final GenericValue arg0, final boolean not, final List<GenericValue> argList) {
        super(arg0, not, argList);
    }

    protected boolean evaluateInList(final Object object) throws HBqlException, ResultMissingColumnException {

        final long attribVal = (Long)this.getArg(0).getValue(object);

        for (final GenericValue obj : this.getInList()) {
            // Check if the value returned is a collection
            final Object objval = obj.getValue(object);
            if (HUtil.isACollection(objval)) {
                for (final GenericValue val : (Collection<GenericValue>)objval) {
                    if (attribVal == (Long)val.getValue(object))
                        return true;
                }
            }
            else {
                if (attribVal == (Long)objval)
                    return true;
            }
        }

        return false;
    }
}