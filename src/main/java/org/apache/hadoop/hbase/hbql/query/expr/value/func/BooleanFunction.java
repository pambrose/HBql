package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 5:28:17 PM
 */
public class BooleanFunction extends GenericFunction {

    public BooleanFunction(final FunctionType functionType, final ValueExpr... valueExprs) {
        super(functionType, valueExprs);
    }

    @Override
    public Object getValue(final Object object) throws HPersistException {

        switch (this.getFunctionType()) {
            case CONTAINS: {
                final String val1 = (String)this.getValueExprs()[0].getValue(object);
                final String val2 = (String)this.getValueExprs()[1].getValue(object);
                if (val1 == null || val2 == null)
                    return false;
                else
                    return val1.contains(val2);
            }

            default:
                throw new HPersistException("Invalid function in BooleanFunction.getValue() " + this.getFunctionType());
        }
    }

}