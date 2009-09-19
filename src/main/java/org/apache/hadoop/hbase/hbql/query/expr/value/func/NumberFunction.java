package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 5:28:17 PM
 */
public class NumberFunction extends GenericFunction implements NumberValue {

    public NumberFunction(final Func func, final StringValue... stringExprs) {
        super(func, stringExprs);
    }

    @Override
    public Number getCurrentValue(final Object object) throws HPersistException {

        switch (this.getFunc()) {
            case LENGTH: {
                final String val = this.getStringExprs()[0].getCurrentValue(object);
                if (val == null)
                    return 0;
                else
                    return val.length();
            }

            default:
                throw new HPersistException("Error in NumberFunction.getValue() " + this.getFunc());
        }

    }

}