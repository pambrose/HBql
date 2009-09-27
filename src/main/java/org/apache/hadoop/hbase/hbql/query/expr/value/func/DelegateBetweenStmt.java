package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class DelegateBetweenStmt extends GenericBetweenStmt {

    private GenericBetweenStmt typedExpr = null;

    public DelegateBetweenStmt(final ValueExpr expr, final boolean not, final ValueExpr lower, final ValueExpr upper) {
        super(not, expr, lower, upper);
    }

    @Override
    public Class<? extends ValueExpr> validateTypes(final ValueExpr parentExpr,
                                                    final boolean allowsCollections) throws TypeException {

        final Class<? extends ValueExpr> type1 = this.getExpr().validateTypes(this, false);
        final Class<? extends ValueExpr> type2 = this.getLower().validateTypes(this, false);
        final Class<? extends ValueExpr> type3 = this.getUpper().validateTypes(this, false);

        if (HUtil.isParentClass(StringValue.class, type1, type2, type3))
            this.typedExpr = new StringBetweenStmt(this.getExpr(), this.isNot(), this.getLower(), this.getUpper());
        else if (HUtil.isParentClass(NumberValue.class, type1, type2, type3))
            this.typedExpr = new NumberBetweenStmt(this.getExpr(), this.isNot(), this.getLower(), this.getUpper());
        else if (HUtil.isParentClass(DateValue.class, type1, type2, type3))
            this.typedExpr = new DateBetweenStmt(this.getExpr(), this.isNot(), this.getLower(), this.getUpper());
        else
            HUtil.throwInvalidTypeException(this, type1, type2, type3);

        this.typedExpr.validateTypes(parentExpr, false);

        return BooleanValue.class;
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