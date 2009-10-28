package org.apache.expreval.expr.var;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.node.StringValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.contrib.hbql.schema.ColumnAttrib;

public class KeyColumn extends GenericColumn<StringValue> {

    public KeyColumn(final ColumnAttrib attrib) {
        super(attrib);
    }

    public Object getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        if (this.getExprContext().useHBaseResult())
            return this.getColumnAttrib().getValueFromBytes((Result)object);
        else
            return this.getColumnAttrib().getCurrentValue(object);
    }
}