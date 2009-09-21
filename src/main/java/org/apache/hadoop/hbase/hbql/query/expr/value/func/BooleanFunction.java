package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 5:28:17 PM
 */
public class BooleanFunction extends GenericFunction implements BooleanValue {

    public BooleanFunction(final Type functionType, final StringValue... stringExprs) {
        super(functionType, stringExprs);
    }

    @Override
    public Boolean getValue(final Object object) throws HPersistException {

        switch (this.getFunctionType()) {
            case CONTAINS: {
                final String val1 = this.getStringExprs()[0].getValue(object);
                final String val2 = this.getStringExprs()[1].getValue(object);
                if (val1 == null || val2 == null)
                    return false;
                else
                    return val1.contains(val2);
            }

            default:
                throw new HPersistException("Error in BooleanFunction.getValue() " + this.getFunctionType());
        }

    }

}