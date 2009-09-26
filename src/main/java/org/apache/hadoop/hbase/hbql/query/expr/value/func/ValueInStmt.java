package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class ValueInStmt extends GenericInStmt {

    private GenericInStmt typedExpr = null;

    public ValueInStmt(final ValueExpr expr, final boolean not, final List<ValueExpr> valList) {
        super(not, expr, valList);
    }

    @Override
    public Class<? extends ValueExpr> validateTypes(final ValueExpr parentExpr,
                                                    final boolean allowsCollections) throws TypeException {

        final Class<? extends ValueExpr> type = this.getExpr().validateTypes(this, false);

        if (HUtil.isParentClass(StringValue.class, type))
            this.typedExpr = new StringInStmt(this.getExpr(), this.isNot(), this.getValueExprList());
        else if (HUtil.isParentClass(NumberValue.class, type))
            this.typedExpr = new NumberInStmt(this.getExpr(), this.isNot(), this.getValueExprList());
        else if (HUtil.isParentClass(DateValue.class, type))
            this.typedExpr = new DateInStmt(this.getExpr(), this.isNot(), this.getValueExprList());
        else
            HUtil.throwInvalidTypeException(this, type);

        this.typedExpr.validateTypes(parentExpr, false);

        return BooleanValue.class;
    }

    @Override
    protected boolean evaluateList(final Object object) throws HBqlException {
        // Not used
        return false;
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