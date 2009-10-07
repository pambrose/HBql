package org.apache.hadoop.hbase.hbql.query.expr.value.var;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;

public class LongColumn extends GenericColumn<NumberValue> implements NumberValue {

    public LongColumn(ColumnAttrib attrib) {
        super(attrib);
    }

    public Long getValue(final Object object) throws HBqlException {
        if (this.getExprContext().useHBaseResult())
            return (Long)this.getColumnAttrib().getValueFromBytes((Result)object);
        else
            return (Long)this.getColumnAttrib().getCurrentValue(object);
    }
}