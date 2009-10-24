package org.apache.hadoop.hbase.hbql.stmt.expr.var;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.NumberValue;

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