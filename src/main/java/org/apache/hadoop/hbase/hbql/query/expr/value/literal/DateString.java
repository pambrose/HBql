package org.apache.hadoop.hbase.hbql.query.expr.value.literal;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.GenericExpr;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class DateString extends GenericExpr implements DateValue {

    public DateString(final GenericValue arg0, final GenericValue arg1) {
        super(Type.DATESTRING, arg0, arg1);
    }

    @Override
    public Long getValue(final Object object) throws HBqlException {

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

    @Override
    public String asString() {
        return "DATE" + super.asString();
    }
}