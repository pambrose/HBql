package org.apache.expreval.expr.var;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.contrib.hbql.schema.ColumnAttrib;

public class ShortColumn extends GenericColumn<NumberValue> implements NumberValue {

    public ShortColumn(ColumnAttrib attrib) {
        super(attrib);
    }

    public Short getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        if (this.getExprContext().useResultData())
            return (Short)this.getColumnAttrib().getValueFromBytes((Result)object);
        else
            return (Short)this.getColumnAttrib().getCurrentValue(object);
    }
}