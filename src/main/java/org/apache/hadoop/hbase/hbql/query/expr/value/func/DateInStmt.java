package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class DateInStmt extends GenericInStmt<DateValue> {

    public DateInStmt(final DateValue expr, final boolean not, final List<DateValue> valueList) {
        super(not, expr, valueList);
    }

    protected boolean evaluateList(final Object object) throws HPersistException {

        final long attribVal = this.getExpr().getValue(object);
        for (final DateValue obj : this.getValueList()) {
            final long val = obj.getValue(object);
            if (attribVal == val)
                return true;
        }

        return false;
    }

}