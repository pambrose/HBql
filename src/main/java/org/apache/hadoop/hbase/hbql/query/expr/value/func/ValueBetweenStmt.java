package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class ValueBetweenStmt extends GenericBetweenStmt<ValueExpr> {

    private GenericBetweenStmt typedExpr = null;

    public ValueBetweenStmt(final ValueExpr expr, final boolean not, final ValueExpr lower, final ValueExpr upper) {
        super(not, expr, lower, upper);
    }

    @Override
    public Class<? extends ValueExpr> validateType() throws HPersistException {

        final Class<? extends ValueExpr> type1 = this.getExpr().validateType();
        final Class<? extends ValueExpr> type2 = this.getLower().validateType();
        final Class<? extends ValueExpr> type3 = this.getUpper().validateType();

        if (!type1.equals(type2) || type1.equals(type3))
            throw new HPersistException("Mismatched types in ValueBetweenStmt");

        if (type1.equals(StringValue.class))
            this.typedExpr = new StringBetweenStmt((StringValue)this.getExpr(),
                                                   this.isNot(),
                                                   (StringValue)this.getLower(),
                                                   (StringValue)this.getUpper());
        else if (type1.equals(NumberValue.class))
            this.typedExpr = new NumberBetweenStmt((NumberValue)this.getExpr(),
                                                   this.isNot(),
                                                   (NumberValue)this.getLower(),
                                                   (NumberValue)this.getUpper());
        else if (type1.equals(DateValue.class))
            this.typedExpr = new DateBetweenStmt((DateValue)this.getExpr(),
                                                 this.isNot(),
                                                 (DateValue)this.getLower(),
                                                 (DateValue)this.getUpper());
        else
            throw new HPersistException("Invalid type " + type1.getName() + " in ValueBetweenStmt");

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