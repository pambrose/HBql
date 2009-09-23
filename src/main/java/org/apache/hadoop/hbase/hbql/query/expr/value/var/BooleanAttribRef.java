package org.apache.hadoop.hbase.hbql.query.expr.value.var;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.schema.FieldType;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class BooleanAttribRef extends GenericAttribRef<NumberValue> implements BooleanValue {

    public BooleanAttribRef(final String attribName) {
        super(attribName, FieldType.BooleanType);
    }

    @Override
    public Boolean getValue(final Object object) throws HPersistException {
        return (Boolean)this.getVariableAttrib().getCurrentValue(object);
    }

}