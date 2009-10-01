package org.apache.hadoop.hbase.hbql.query.expr.value.var;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class StringColumn extends GenericColumn<StringValue> {

    public StringColumn(final ColumnAttrib attrib) {
        super(attrib);
    }

    @Override
    public Object getValue(final Object object) throws HBqlException {
        if (this.getExprContext().useHBaseResult())
            return this.getColumnAttrib().getValueFromBytes((Result)object);
        else
            return this.getColumnAttrib().getCurrentValue(object);
    }
}