package org.apache.hadoop.hbase.hbql.query.expr.value.var;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.schema.VariableAttrib;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class StringColumn extends GenericColumn<StringValue> {

    public StringColumn(final VariableAttrib attrib) {
        super(attrib);
    }

    @Override
    public Object getValue(final Object object) throws HBqlException {
        return this.getVariableAttrib().getCurrentValue(object);
    }
}