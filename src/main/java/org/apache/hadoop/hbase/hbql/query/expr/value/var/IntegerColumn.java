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
public class IntegerColumn extends GenericColumn<NumberValue> implements NumberValue {

    public IntegerColumn(ColumnAttrib attrib) {
        super(attrib);
    }

    @Override
    public Integer getValue(final Object object) throws HBqlException {
        return (Integer)this.getColumnAttrib().getCurrentValue(object);
    }

}