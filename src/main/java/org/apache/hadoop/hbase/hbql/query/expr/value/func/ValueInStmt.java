package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class ValueInStmt extends GenericInStmt<ValueExpr> {

    private GenericInStmt typedExpr = null;

    public ValueInStmt(final ValueExpr expr, final boolean not, final List<ValueExpr> valList) {
        super(not, expr, valList);
    }

    @Override
    public Class<? extends ValueExpr> validateType() throws HPersistException {

        final Class<? extends ValueExpr> type = this.getExpr().validateType();

        // First make sure all the types are matched
        for (final ValueExpr val : this.getValueList()) {
            final Class<? extends ValueExpr> valtype = val.validateType();

            if (!valtype.equals(type))
                throw new HPersistException("Mismatched " + valtype.getName() + " in GenericInStmt");
        }

        if (type.equals(StringValue.class)) {
            this.typedExpr = new StringInStmt(this.getExpr(), this.isNot(), this.getValueList());
        }
        else if (type.equals(NumberValue.class)) {
            this.typedExpr = new NumberInStmt(this.getExpr(), this.isNot(), this.getValueList());
        }
        else if (type.equals(DateValue.class)) {
            this.typedExpr = new DateInStmt(this.getExpr(), this.isNot(), this.getValueList());
        }
        else
            throw new HPersistException("Invalid type " + type.getName() + " in GenericInStmt");

        return BooleanValue.class;
    }

    @Override
    protected boolean evaluateList(final Object object) throws HPersistException {
        // Not used
        return false;
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