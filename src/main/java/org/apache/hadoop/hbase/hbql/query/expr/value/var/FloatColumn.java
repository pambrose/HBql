package org.apache.hadoop.hbase.hbql.query.expr.value.var;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;

public class FloatColumn extends GenericColumn<NumberValue> implements NumberValue {

    public FloatColumn(ColumnAttrib attrib) {
        super(attrib);
    }

    public Float getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        if (this.getExprContext().useHBaseResult())
            return (Float)this.getColumnAttrib().getValueFromBytes((Result)object);
        else
            return (Float)this.getColumnAttrib().getCurrentValue(object);
    }
}