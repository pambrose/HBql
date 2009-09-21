package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.StringLiteral;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class StringInStmt extends GenericInStmt<StringValue> {

    public StringInStmt(final StringValue expr, final boolean not, final List<StringValue> valList) {
        super(not, expr, valList);
    }

    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {

        boolean retval = true;

        if (this.getExpr().optimizeForConstants(object))
            this.setExpr(new StringLiteral(this.getExpr().getValue(object)));
        else
            retval = false;

        if (!this.optimizeList(object))
            retval = false;

        return retval;
    }

    private boolean optimizeList(final Object object) throws HPersistException {

        boolean retval = true;
        final List<StringValue> newvalList = Lists.newArrayList();

        for (final StringValue val : this.getValueList()) {
            if (val.optimizeForConstants(object)) {
                newvalList.add(new StringLiteral(val.getValue(object)));
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

        final String attribVal = this.getExpr().getValue(object);
        for (final StringValue obj : this.getValueList()) {
            final String val = obj.getValue(object);
            if (attribVal.equals(val))
                return true;
        }

        return false;
    }

}