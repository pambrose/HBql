package org.apache.hadoop.hbase.hbql.query.expr.value.var;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.schema.FieldType;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class StringAttribRef extends GenericAttribRef<StringValue> {

    public StringAttribRef(final String attribName) {
        super(attribName, FieldType.StringType);
    }

    @Override
    public Object getValue(final Object object) throws HPersistException {
        return this.getVariableAttrib().getCurrentValue(object);
    }
}