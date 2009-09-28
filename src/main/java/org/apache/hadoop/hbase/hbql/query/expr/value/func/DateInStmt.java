package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

import java.util.Collection;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class DateInStmt extends GenericInStmt {

    public DateInStmt(final GenericValue arg0, final boolean not, final List<GenericValue> argList) {
        super(arg0, not, argList);
    }

    protected boolean evaluateList(final Object object) throws HBqlException {

        final long attribVal = (Long)this.getArg(0).getValue(object);

        for (final GenericValue obj : this.getInList()) {

            // Check if the value returned is a collection
            final Object objval = obj.getValue(object);
            if (HUtil.isParentClass(Collection.class, objval.getClass())) {
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