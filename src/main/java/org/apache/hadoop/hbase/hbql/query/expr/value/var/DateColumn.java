package org.apache.hadoop.hbase.hbql.query.expr.value.var;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;

import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class DateColumn extends GenericColumn<DateValue> implements DateValue {

    public DateColumn(final ColumnAttrib attrib) {
        super(attrib);
    }

    @Override
    public Long getValue(final Object object) throws HBqlException {

        final Date val;

        if (this.getExprContext().useHBaseResult())
            val = (Date)this.getColumnAttrib().getValueFromBytes((Result)object);
        else
            val = (Date)this.getColumnAttrib().getCurrentValue(object);

        return val.getTime();
    }
}