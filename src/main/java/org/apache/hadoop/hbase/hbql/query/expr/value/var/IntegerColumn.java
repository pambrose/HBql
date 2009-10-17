package org.apache.hadoop.hbase.hbql.query.expr.value.var;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;

public class IntegerColumn extends GenericColumn<NumberValue> implements NumberValue {

    public IntegerColumn(ColumnAttrib attrib) {
        super(attrib);
    }

    public Integer getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        if (this.getExprContext().useHBaseResult())
            return (Integer)this.getColumnAttrib().getValueFromBytes((Result)object);
        else
            return (Integer)this.getColumnAttrib().getCurrentValue(object);
    }
}