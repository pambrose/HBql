package org.apache.hadoop.hbase.hbql.query.expr.value.var;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.schema.VariableAttrib;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 29, 2009
 * Time: 1:13:05 PM
 */
public class DelegateColumn extends GenericColumn<GenericValue> {

    private GenericColumn typedColumn = null;

    public DelegateColumn(final VariableAttrib attrib) {
        super(attrib, null);
    }

    @Override
    public Object getValue(final Object object) throws HBqlException {
        return this.typedColumn.getValue(object);
    }
}
