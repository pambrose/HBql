package org.apache.hadoop.hbase.hbql.query.expr.value.function;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;

import java.util.List;

public class NumberFunction extends Function implements NumberValue {

    public NumberFunction(final FunctionType functionType, final GenericValue... exprs) {
        super(functionType, exprs);
    }

    public NumberFunction(final FunctionType functionType, final List<GenericValue> exprs) {
        super(functionType, exprs);
    }

    public Number getValue(final Object object) throws HBqlException, ResultMissingColumnException {

        switch (this.getFunctionType()) {

            case LENGTH: {
                final String val = (String)this.getArg(0).getValue(object);
                this.checkForNull(val);
                return val.length();
            }

            case INDEXOF: {
                final String v1 = (String)this.getArg(0).getValue(object);
                final String v2 = (String)this.getArg(1).getValue(object);
                this.checkForNull(v1, v2);
                return v1.indexOf(v2);
            }

            case DATETOLONG: {
                return (Long)this.getArg(0).getValue(object);
            }

            case SHORT: {
                final String v1 = (String)this.getArg(0).getValue(object);
                return Short.valueOf(v1);
            }

            case INTEGER: {
                final String v1 = (String)this.getArg(0).getValue(object);
                return Integer.valueOf(v1);
            }

            case LONG: {
                final String v1 = (String)this.getArg(0).getValue(object);
                return Long.valueOf(v1);
            }

            case FLOAT: {
                final String v1 = (String)this.getArg(0).getValue(object);
                return Float.valueOf(v1);
            }

            case DOUBLE: {
                final String v1 = (String)this.getArg(0).getValue(object);
                return Double.valueOf(v1);
            }

            default:
                throw new HBqlException("Invalid function: " + this.getFunctionType());
        }
    }
}