package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class NumberInStmt extends GenericInStmt {

    public NumberInStmt(final ValueExpr expr, final boolean not, final List<ValueExpr> vals) {
        super(not, expr, vals);
    }

    protected boolean evaluateList(final Object object) throws HPersistException {

        final long attribVal = ((Number)this.getExpr().getValue(object)).longValue();
        for (final ValueExpr obj : this.getValueList()) {
            final long val = ((Number)obj.getValue(object)).longValue();
            if (attribVal == val)
                return true;
        }
        return false;
    }
}