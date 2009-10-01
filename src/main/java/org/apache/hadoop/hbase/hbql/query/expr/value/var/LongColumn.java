package org.apache.hadoop.hbase.hbql.query.expr.value.var;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class LongColumn extends GenericColumn<NumberValue> implements NumberValue {

    public LongColumn(ColumnAttrib attrib) {
        super(attrib);
    }

    @Override
    public Long getValue(final Object object) throws HBqlException {
        return (Long)this.getColumnAttrib().getCurrentValue(object);
    }

}