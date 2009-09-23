package org.apache.hadoop.hbase.hbql.query.expr.value.var;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.schema.VariableAttrib;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class ValueAttribRef extends GenericAttribRef<ValueExpr> {

    private GenericAttribRef typedExpr = null;

    public ValueAttribRef(final String attribName) {
        super(attribName, null);
    }

    @Override
    public Class<? extends ValueExpr> validateType() throws HPersistException {

        final VariableAttrib attrib = this.getVariableAttrib();

        final Class<? extends ValueExpr> clazz = attrib.getFieldType().getExprType();

        // TODO Need to add Long support to this
        if (clazz.equals(DateValue.class))
            typedExpr = new DateAttribRef(this.getName());
        else if (clazz.equals(StringValue.class))
            typedExpr = new StringAttribRef(this.getName());
        else if (clazz.equals(NumberValue.class))
            typedExpr = new IntegerAttribRef(this.getName());
        else
            throw new HPersistException("Invalid type " + clazz.getName() + " in ValueAttribRef.validateType()");

        return clazz;
    }

    @Override
    public ValueExpr getOptimizedValue() throws HPersistException {
        return this.typedExpr.getOptimizedValue();
    }

    @Override
    public Object getValue(final Object object) throws HPersistException {
        return this.typedExpr.getValue(object);
    }
}