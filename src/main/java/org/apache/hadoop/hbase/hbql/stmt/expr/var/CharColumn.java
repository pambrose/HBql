package org.apache.hadoop.hbase.hbql.stmt.expr.var;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.stmt.schema.ColumnAttrib;

public class CharColumn extends GenericColumn<NumberValue> implements NumberValue {

    public CharColumn(ColumnAttrib attrib) {
        super(attrib);
    }

    public Short getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        if (this.getExprContext().useHBaseResult())
            return (Short)this.getColumnAttrib().getValueFromBytes((Result)object);
        else
            return (Short)this.getColumnAttrib().getCurrentValue(object);
    }
}