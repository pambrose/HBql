package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 5:28:17 PM
 */
public class StringFunction extends GenericFunction implements StringValue {

    public StringFunction(final Func func, final StringValue... stringExprs) {
        super(func, stringExprs);
    }

    @Override
    public String getCurrentValue(final Object object) throws HPersistException {

        switch (this.getFunc()) {
            case TRIM: {
                final String val = this.getStringExprs()[0].getCurrentValue(object);
                return val.trim();
            }

            case LOWER: {
                final String val = this.getStringExprs()[0].getCurrentValue(object);
                return val.toLowerCase();
            }

            case UPPER: {
                final String val = this.getStringExprs()[0].getCurrentValue(object);
                return val.toUpperCase();
            }

            case CONCAT: {
                final String v1 = this.getStringExprs()[0].getCurrentValue(object);
                final String v2 = this.getStringExprs()[1].getCurrentValue(object);
                return v1 + v2;
            }

            default:
                throw new HPersistException("Error in StringFunction.getValue() " + this.getFunc());
        }
    }
}
