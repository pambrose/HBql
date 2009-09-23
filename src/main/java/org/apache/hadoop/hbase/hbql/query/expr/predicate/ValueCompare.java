package org.apache.hadoop.hbase.hbql.query.expr.predicate;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public class ValueCompare extends GenericCompare<ValueExpr> {

    private GenericCompare typedExpr = null;

    public ValueCompare(final ValueExpr expr1, final OP op, final ValueExpr expr2) {
        super(expr1, op, expr2);
    }

    @Override
    public ValueExpr getOptimizedValue() throws HPersistException {

        this.setExpr1(this.getExpr1().getOptimizedValue());
        this.setExpr2(this.getExpr2().getOptimizedValue());

        return this.isAConstant() ? new BooleanLiteral(this.getValue(null)) : this;
    }

    @Override
    public Class<? extends ValueExpr> validateType() throws HPersistException {

        final Class<? extends ValueExpr> type1 = this.getExpr1().validateType();
        final Class<? extends ValueExpr> type2 = this.getExpr2().validateType();

        if (!ExprTree.isOfType(type1, StringValue.class, NumberValue.class, DateValue.class))
            throw new HPersistException("Type " + type1.getName() + " not valid in ValueCompare");

        if (!ExprTree.isOfType(type2, StringValue.class, NumberValue.class, DateValue.class))
            throw new HPersistException("Type " + type2.getName() + " not valid in ValueCompare");

        if (type1.equals(DateValue.class))
            typedExpr = new DateCompare((DateValue)this.getExpr1(), this.getOp(), (DateValue)this.getExpr2());
        else if (type1.equals(StringValue.class))
            typedExpr = new StringCompare((StringValue)this.getExpr1(), this.getOp(), (StringValue)this.getExpr2());
        else if (type1.equals(NumberValue.class))
            typedExpr = new NumberCompare((NumberValue)this.getExpr1(), this.getOp(), (NumberValue)this.getExpr2());
        else
            typedExpr = null;  // Never executed

        return BooleanValue.class;
    }

    @Override
    public Boolean getValue(final Object object) throws HPersistException {
        return this.typedExpr.getValue(object);
    }

}