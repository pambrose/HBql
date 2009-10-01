package org.apache.hadoop.hbase.hbql.query.expr.value.var;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.schema.VariableAttrib;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class BooleanColumn extends GenericColumn<NumberValue> implements BooleanValue {

    public BooleanColumn(final VariableAttrib attrib) {
        super(attrib);
    }

    @Override
    public Boolean getValue(final Object object) throws HBqlException {
        return (Boolean)this.getVariableAttrib().getCurrentValue(object);
    }

}