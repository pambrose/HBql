package org.apache.hadoop.hbase.hbql.query.expr.predicate;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.func.Operator;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public class ValueCompare extends GenericCompare<ValueExpr> {

    private GenericCompare typedExpr = null;

    public ValueCompare(final ValueExpr expr1, final Operator op, final ValueExpr expr2) {
        super(expr1, op, expr2);
    }

    @Override
    public Class<? extends ValueExpr> validateType() throws HPersistException {

        final Class<? extends ValueExpr> type1 = this.getExpr1().validateType();
        final Class<? extends ValueExpr> type2 = this.getExpr2().validateType();

        if (!type1.equals(type2))
            throw new HPersistException("Type mismatch in ValueCompare");

        if (!ExprTree.isOfType(type1, StringValue.class, NumberValue.class, DateValue.class))
            throw new HPersistException("Invalid type " + type1.getName() + " in ValueCompare");

        if (type1.equals(DateValue.class))
            typedExpr = new DateCompare(this.getExpr1(), this.getOp(), this.getExpr2());
        else if (type1.equals(StringValue.class))
            typedExpr = new StringCompare(this.getExpr1(), this.getOp(), this.getExpr2());
        else if (type1.equals(NumberValue.class))
            typedExpr = new NumberCompare(this.getExpr1(), this.getOp(), this.getExpr2());
        else
            typedExpr = null;  // Never executed

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