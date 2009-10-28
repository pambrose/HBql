package org.apache.expreval.expr.var;

import org.apache.expreval.expr.node.NumberValue;
import org.apache.expreval.schema.ColumnAttrib;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.ResultMissingColumnException;

public class LongColumn extends GenericColumn<NumberValue> implements NumberValue {

    public LongColumn(ColumnAttrib attrib) {
        super(attrib);
    }

    public Long getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        if (this.getExprContext().useHBaseResult())
            return (Long)this.getColumnAttrib().getValueFromBytes((Result)object);
        else
            return (Long)this.getColumnAttrib().getCurrentValue(object);
    }
}