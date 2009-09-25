package org.apache.hadoop.hbase.hbql.query.expr.value.var;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.schema.FieldType;
import org.apache.hadoop.hbase.hbql.query.schema.VariableAttrib;

import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class DateAttribRef extends GenericAttribRef<DateValue> implements DateValue {

    public DateAttribRef(final VariableAttrib attrib) {
        super(attrib, FieldType.DateType);
    }

    @Override
    public Long getValue(final Object object) throws HPersistException {
        return ((Date)this.getVariableAttrib().getCurrentValue(object)).getTime();
    }

}