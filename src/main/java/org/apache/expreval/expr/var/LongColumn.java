package org.apache.expreval.expr.var;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.contrib.hbql.schema.ColumnAttrib;

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