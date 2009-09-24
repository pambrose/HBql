package org.apache.hadoop.hbase.hbql.query.expr.value.var;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class NamedParameter extends GenericAttribRef<ValueExpr> {

    private ValueExpr typedExpr = null;

    public NamedParameter(final String attribName) {
        super(attribName, null);
    }

    @Override
    public Class<? extends ValueExpr> validateType() throws HPersistException {

        if (this.typedExpr == null)
            throw new HPersistException("Parameter " + this.getName() + " not assigned");

        return this.typedExpr.getClass();
    }

    @Override
    public ValueExpr getOptimizedValue() throws HPersistException {
        return this.typedExpr.getOptimizedValue();
    }

    @Override
    public Object getValue(final Object object) throws HPersistException {
        return this.typedExpr.getValue(object);
    }

    @Override
    public void setParam(final String param, final Object val) {

        if (param.startsWith(":"))
            if (!param.equals(this.getName()))
                return;
            else if (!(":" + param).equals(this.getName()))
                return;

        if (val instanceof Boolean) {
            this.typedExpr = new BooleanLiteral((Boolean)val);
            return;
        }

    }

}