package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public class ValueNullCompare extends GenericNullCompare {

    private GenericNullCompare typedExpr = null;

    public ValueNullCompare(final boolean not, final ValueExpr expr) {
        super(not, expr);
    }

    public Class<? extends ValueExpr> validateType() throws HPersistException {

        final Class<? extends ValueExpr> type = this.getExpr().validateType();

        if (!type.equals(StringValue.class))
            throw new HPersistException("Invalid type " + type.getName() + " in ValueNullCompare");

        this.typedExpr = new StringNullCompare(this.isNot(), this.getExpr());

        return BooleanValue.class;
    }

    @Override
    public ValueExpr getOptimizedValue() throws HPersistException {
        return this.typedExpr.getOptimizedValue();
    }

    @Override
    public Boolean getValue(final Object object) throws HPersistException {
        return this.typedExpr.getValue(object);
    }
}