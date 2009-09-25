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
public class StringInStmt extends GenericInStmt {

    public StringInStmt(final ValueExpr expr, final boolean not, final List<ValueExpr> valList) {
        super(not, expr, valList);
    }

    protected boolean evaluateList(final Object object) throws HBqlException {

        final String attribVal = (String)this.getExpr().getValue(object);
        for (final ValueExpr obj : this.getValueExprList()) {
            final String val = (String)obj.getValue(object);
            if (attribVal.equals(val))
                return true;
        }
        return false;
    }
}