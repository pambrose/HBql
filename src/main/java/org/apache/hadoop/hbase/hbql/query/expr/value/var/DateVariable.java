package org.apache.hadoop.hbase.hbql.query.expr.value.var;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
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
public class DateVariable extends GenericVariable<DateValue> implements DateValue {

    public DateVariable(final VariableAttrib attrib) {
        super(attrib, FieldType.DateType);
    }

    @Override
    public Long getValue(final Object object) throws HBqlException {
        return ((Date)this.getVariableAttrib().getCurrentValue(object)).getTime();
    }

}