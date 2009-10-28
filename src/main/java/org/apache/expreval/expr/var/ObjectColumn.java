package org.apache.expreval.expr.var;

import org.apache.expreval.expr.node.NumberValue;
import org.apache.expreval.expr.node.ObjectValue;
import org.apache.expreval.schema.ColumnAttrib;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.ResultMissingColumnException;

public class ObjectColumn extends GenericColumn<NumberValue> implements ObjectValue {

    public ObjectColumn(ColumnAttrib attrib) {
        super(attrib);
    }

    public Object getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        if (this.getExprContext().useHBaseResult())
            return this.getColumnAttrib().getValueFromBytes((Result)object);
        else
            return this.getColumnAttrib().getCurrentValue(object);
    }
}