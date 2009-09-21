package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.DateLiteral;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

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

    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {

        boolean retval = true;

        if (this.getExpr().optimizeForConstants(object))
            this.setExpr(new DateLiteral(this.getExpr().getValue(object)));
        else
            retval = false;

        if (!this.optimizeList(object))
            retval = false;

        return retval;
    }

    private boolean optimizeList(final Object object) throws HPersistException {

        boolean retval = true;
        final List<DateValue> newvalList = Lists.newArrayList();

        for (final DateValue val : this.getValueList()) {
            if (val.optimizeForConstants(object)) {
                newvalList.add(new DateLiteral(val.getValue(object)));
            }
            else {
                newvalList.add(val);
                retval = false;
            }
        }

        // Swap new values to list
        this.getValueList().clear();
        this.getValueList().addAll(newvalList);

        return retval;
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