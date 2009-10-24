package org.apache.hadoop.hbase.hbql.stmt.expr.instmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.stmt.util.HUtil;

import java.util.Collection;
import java.util.List;

public class NumberInStmt extends GenericInStmt {

    public NumberInStmt(final GenericValue arg0, final boolean not, final List<GenericValue> inList) {
        super(arg0, not, inList);
    }

    protected boolean evaluateInList(final Object object) throws HBqlException, ResultMissingColumnException {

        final Object obj0 = this.getArg(0).getValue(object);

        this.validateNumericArgTypes(obj0);

        if (!this.useDecimal()) {

            final long val0 = ((Number)obj0).longValue();

            for (final GenericValue obj : this.getInList()) {

                // Check if the value returned is a collection
                final Object objval = obj.getValue(object);
                if (HUtil.isACollection(objval)) {
                    for (final GenericValue genericValue : (Collection<GenericValue>)objval) {
                        if (val0 == ((Number)genericValue.getValue(object)).longValue())
                            return true;
                    }
                }
                else {
                    if (val0 == ((Number)objval).longValue())
                        return true;
                }
            }
            return false;
        }
        else {

            final double val0 = ((Number)obj0).doubleValue();

            for (final GenericValue obj : this.getInList()) {

                // Check if the value returned is a collection
                final Object objval = obj.getValue(object);
                if (HUtil.isACollection(objval)) {
                    for (final GenericValue genericValue : (Collection<GenericValue>)objval) {
                        if (val0 == ((Number)genericValue.getValue(object)).doubleValue())
                            return true;
                    }
                }
                else {
                    if (val0 == ((Number)objval).doubleValue())
                        return true;
                }
            }
            return false;
        }
    }
}