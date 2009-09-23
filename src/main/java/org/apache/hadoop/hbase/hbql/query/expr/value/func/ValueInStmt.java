package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class ValueInStmt extends GenericInStmt<ValueExpr> {

    public ValueInStmt(final StringValue expr, final boolean not, final List<StringValue> valList) {
        super(not, expr, valList);
    }

    @Override
    protected Class<? extends ValueExpr> getClassType() throws HPersistException {
        return StringValue.class;
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