package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class DateInStmt extends GenericInStmt<ValueExpr> {

    public DateInStmt(final ValueExpr expr, final boolean not, final List<ValueExpr> valueList) {
        super(not, expr, valueList);
    }

    protected boolean evaluateList(final Object object) throws HPersistException {

        final long attribVal = ((DateValue)this.getExpr()).getValue(object);
        for (final ValueExpr obj : this.getValueList()) {
            final long val = ((DateValue)obj).getValue(object);
            if (attribVal == val)
                return true;
        }

        return false;
    }

}