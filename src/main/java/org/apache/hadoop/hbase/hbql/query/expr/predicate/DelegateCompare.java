package org.apache.hadoop.hbase.hbql.query.expr.predicate;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.func.Operator;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public class DelegateCompare extends GenericCompare implements BooleanValue {

    private GenericCompare typedExpr = null;

    public DelegateCompare(final ValueExpr expr1, final Operator op, final ValueExpr expr2) {
        super(expr1, op, expr2);
    }

    @Override
    public Class<? extends ValueExpr> validateTypes(final ValueExpr parentExpr,
                                                    final boolean allowsCollections) throws TypeException {

        final Class<? extends ValueExpr> type1 = this.getExpr1().validateTypes(this, false);
        final Class<? extends ValueExpr> type2 = this.getExpr2().validateTypes(this, false);

        if (HUtil.isParentClass(StringValue.class, type1, type2))
            typedExpr = new StringCompare(this.getExpr1(), this.getOp(), this.getExpr2());
        else if (HUtil.isParentClass(NumberValue.class, type1, type2))
            typedExpr = new NumberCompare(this.getExpr1(), this.getOp(), this.getExpr2());
        else if (HUtil.isParentClass(DateValue.class, type1, type2))
            typedExpr = new DateCompare(this.getExpr1(), this.getOp(), this.getExpr2());
        else if (HUtil.isParentClass(BooleanValue.class, type1, type2))
            typedExpr = new BooleanCompare(this.getExpr1(), this.getOp(), this.getExpr2());
        else
            HUtil.throwInvalidTypeException(this, type1, type2);

        return this.typedExpr.validateTypes(parentExpr, allowsCollections);
    }

    @Override
    public ValueExpr getOptimizedValue() throws HBqlException {
        return this.typedExpr.getOptimizedValue();
    }

    @Override
    public Boolean getValue(final Object object) throws HBqlException {
        return this.typedExpr.getValue(object);
    }

}