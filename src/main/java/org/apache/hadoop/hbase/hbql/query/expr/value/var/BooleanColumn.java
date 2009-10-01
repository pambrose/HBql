package org.apache.hadoop.hbase.hbql.query.expr.value.var;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class BooleanColumn extends GenericColumn<NumberValue> implements BooleanValue {

    public BooleanColumn(final ColumnAttrib attrib) {
        super(attrib);
    }

    @Override
    public Boolean getValue(final Object object) throws HBqlException {
        if (this.getExprContext().useHBaseResult())
            return (Boolean)this.getColumnAttrib().getValueFromBytes((Result)object);
        else
            return (Boolean)this.getColumnAttrib().getCurrentValue(object);
    }
}