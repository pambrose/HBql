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
public class DateAttribRef extends GenericAttribRef implements DateValue {

    public DateAttribRef(final String attribName) {
        super(attribName, FieldType.DateType);
    }

    @Override
    public Long getCurrentValue(final Object object) throws HPersistException {
        final VariableAttrib attrib = this.getSchema().getVariableAttribByVariableName(this.getExprVar().getName());
        if (attrib == null)
            throw new HPersistException("Invalid variable name: " + this.getExprVar().getName());
        return ((Date)attrib.getCurrentValue(object)).getTime();
    }

}