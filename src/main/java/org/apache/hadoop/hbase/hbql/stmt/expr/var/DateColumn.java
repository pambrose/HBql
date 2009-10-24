package org.apache.hadoop.hbase.hbql.stmt.expr.var;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.stmt.schema.ColumnAttrib;

import java.util.Date;

public class DateColumn extends GenericColumn<DateValue> implements DateValue {

    public DateColumn(final ColumnAttrib attrib) {
        super(attrib);
    }

    public Long getValue(final Object object) throws HBqlException, ResultMissingColumnException {

        final Date val;

        if (this.getExprContext().useHBaseResult())
            val = (Date)this.getColumnAttrib().getValueFromBytes((Result)object);
        else
            val = (Date)this.getColumnAttrib().getCurrentValue(object);

        return val.getTime();
    }
}