package org.apache.expreval.expr.var;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.contrib.hbql.schema.ColumnAttrib;

public class FloatColumn extends GenericColumn<NumberValue> implements NumberValue {

    public FloatColumn(ColumnAttrib attrib) {
        super(attrib);
    }

    public Float getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        if (this.getExprContext().useResultData())
            return (Float)this.getColumnAttrib().getValueFromBytes((Result)object);
        else
            return (Float)this.getColumnAttrib().getCurrentValue(object);
    }
}