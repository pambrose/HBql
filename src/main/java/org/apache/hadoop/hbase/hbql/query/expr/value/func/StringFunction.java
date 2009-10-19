package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.util.HUtil;

public class StringFunction extends Function implements StringValue {


    public StringFunction(final Type functionType, final GenericValue... exprs) {
        super(functionType, exprs);
    }


    public String getValue(final Object object) throws HBqlException, ResultMissingColumnException {

        switch (this.getFunctionType()) {

            case TRIM: {
                final String val = (String)this.getArg(0).getValue(object);
                this.checkForNull(val);
                return val.trim();
            }

            case LOWER: {
                final String val = (String)this.getArg(0).getValue(object);
                this.checkForNull(val);
                return val.toLowerCase();
            }

            case UPPER: {
                final String val = (String)this.getArg(0).getValue(object);
                this.checkForNull(val);
                return val.toUpperCase();
            }

            case CONCAT: {
                final String v1 = (String)this.getArg(0).getValue(object);
                final String v2 = (String)this.getArg(1).getValue(object);
                this.checkForNull(v1, v2);
                return v1 + v2;
            }

            case REPLACE: {
                final String v1 = (String)this.getArg(0).getValue(object);
                final String v2 = (String)this.getArg(1).getValue(object);
                final String v3 = (String)this.getArg(2).getValue(object);
                this.checkForNull(v1, v2, v3);
                return v1.replace(v2, v3);
            }

            case SUBSTRING: {
                final String val = (String)this.getArg(0).getValue(object);
                final int begin = ((Number)this.getArg(1).getValue(object)).intValue();
                final int end = ((Number)this.getArg(2).getValue(object)).intValue();
                this.checkForNull(val);
                return val.substring(begin, end);
            }

            case ZEROPAD: {
                final int num = ((Number)this.getArg(0).getValue(object)).intValue();
                final int width = ((Number)this.getArg(1).getValue(object)).intValue();
                return HUtil.getZeroPaddedNumber(num, width);
            }

            default:
                throw new HBqlException("Invalid function: " + this.getFunctionType());
        }
    }
}