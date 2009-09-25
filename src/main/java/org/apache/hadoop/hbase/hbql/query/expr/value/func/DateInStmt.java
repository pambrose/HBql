package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class DateInStmt extends GenericInStmt {

    public DateInStmt(final ValueExpr expr, final boolean not, final List<ValueExpr> valueList) {
        super(not, expr, valueList);
    }

    protected boolean evaluateList(final Object object) throws HBqlException {

        final long attribVal = (Long)this.getExpr().getValue(object);
        for (final ValueExpr obj : this.getValueExprList()) {
            final long val = (Long)obj.getValue(object);
            if (attribVal == val)
                return true;
        }

        return false;
    }

}