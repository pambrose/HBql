package org.apache.expreval.expr.var;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.contrib.hbql.schema.ColumnAttrib;

public class BooleanColumn extends GenericColumn<NumberValue> implements BooleanValue {

    public BooleanColumn(final ColumnAttrib attrib) {
        super(attrib);
    }

    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        if (this.getExprContext().useHBaseResult())
            return (Boolean)this.getColumnAttrib().getValueFromBytes((Result)object);
        else
            return (Boolean)this.getColumnAttrib().getCurrentValue(object);
    }
}