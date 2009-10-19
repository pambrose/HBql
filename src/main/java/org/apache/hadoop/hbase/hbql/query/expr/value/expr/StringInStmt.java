package org.apache.hadoop.hbase.hbql.query.expr.value.expr;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.util.HUtil;

import java.util.Collection;
import java.util.List;

public class StringInStmt extends GenericInStmt {

    public StringInStmt(final GenericValue arg0, final boolean not, final List<GenericValue> inList) {
        super(arg0, not, inList);
    }

    protected boolean evaluateList(final Object object) throws HBqlException, ResultMissingColumnException {

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