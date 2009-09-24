package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 5:28:17 PM
 */
public class StringFunction extends GenericFunction {

    public StringFunction(final FunctionType functionType, final ValueExpr... valueExprs) {
        super(functionType, valueExprs);
    }

    @Override
    public String getValue(final Object object) throws HPersistException {

        switch (this.getFunctionType()) {
            case TRIM: {
                final String val = (String)this.getValueExprs()[0].getValue(object);
                return val.trim();
            }

            case LOWER: {
                final String val = (String)this.getValueExprs()[0].getValue(object);
                return val.toLowerCase();
            }

            case UPPER: {
                final String val = (String)this.getValueExprs()[0].getValue(object);
                return val.toUpperCase();
            }

            case CONCAT: {
                final String v1 = (String)this.getValueExprs()[0].getValue(object);
                final String v2 = (String)this.getValueExprs()[1].getValue(object);
                return v1 + v2;
            }

            case REPLACE: {
                final String val = (String)this.getValueExprs()[0].getValue(object);
                final String v2 = (String)this.getValueExprs()[1].getValue(object);
                final String v3 = (String)this.getValueExprs()[2].getValue(object);
                return val.replace(v2, v3);
            }

            case SUBSTRING: {
                final String val = (String)this.getValueExprs()[0].getValue(object);
                final int begin = ((Number)this.getValueExprs()[1].getValue(object)).intValue();
                final int end = ((Number)this.getValueExprs()[2].getValue(object)).intValue();
                return val.substring(begin, end);
            }

        }

        throw new HPersistException("Invalid function in StringFunction.getValue() " + this.getFunctionType());
    }
}
