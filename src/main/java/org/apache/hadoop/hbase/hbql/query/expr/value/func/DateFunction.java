package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class DateFunction extends Function implements DateValue {


    public DateFunction(final Type functionType, final GenericValue... exprs) {
        super(functionType, exprs);
    }


    public Long getValue(final Object object) throws HBqlException, ResultMissingColumnException {

        switch (this.getFunctionType()) {

            case DATE: {
                final String datestr = (String)this.getArg(0).getValue(object);
                final String pattern = (String)this.getArg(1).getValue(object);
                final SimpleDateFormat formatter = new SimpleDateFormat(pattern);

                try {
                    return formatter.parse(datestr).getTime();
                }
                catch (ParseException e) {
                    throw new HBqlException(e.getMessage());
                }
            }

            default:
                throw new HBqlException("Invalid function: " + this.getFunctionType());
        }
    }
}