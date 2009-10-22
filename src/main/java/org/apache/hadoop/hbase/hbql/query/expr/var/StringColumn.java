package org.apache.hadoop.hbase.hbql.query.expr.var;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;

public class StringColumn extends GenericColumn<StringValue> {

    public StringColumn(final ColumnAttrib attrib) {
        super(attrib);
    }

    public Object getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        if (this.getExprContext().useHBaseResult())
            return this.getColumnAttrib().getValueFromBytes((Result)object);
        else
            return this.getColumnAttrib().getCurrentValue(object);
    }
}