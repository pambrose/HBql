package org.apache.hadoop.hbase.hbql.query.expr.predicate;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.GenericOneExprExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 8:28:06 PM
 */
public class CondFactor extends GenericOneExprExpr implements BooleanValue {

    private final boolean not;

    public CondFactor(final boolean not, final BooleanValue expr) {
        super(expr);
        this.not = not;
    }

    public Class<? extends ValueExpr> validateType() throws HPersistException {

        final Class<? extends ValueExpr> type = this.getExpr().validateType();

        if (!type.equals(BooleanValue.class))
            throw new HPersistException("Invalid type " + type.getName() + " in CondFactor");

        return BooleanValue.class;
    }

    @Override
    public ValueExpr getOptimizedValue() throws HPersistException {
        this.setExpr(this.getExpr().getOptimizedValue());
        return this.isAConstant() ? new BooleanLiteral(this.getValue(null)) : this;
    }

    @Override
    public Boolean getValue(final Object object) throws HPersistException {
        final boolean retval = (Boolean)this.getExpr().getValue(object);
        return (this.not) ? !retval : retval;

    }
}
