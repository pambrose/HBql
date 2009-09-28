package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class DelegateBetweenStmt extends GenericBetweenStmt {

    private GenericBetweenStmt typedExpr = null;

    public DelegateBetweenStmt(final GenericValue expr, final boolean not, final GenericValue lower, final GenericValue upper) {
        super(not, expr, lower, upper);
    }

    @Override
    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {

        final Class<? extends GenericValue> type1 = this.getArg(0).validateTypes(this, false);
        final Class<? extends GenericValue> type2 = this.getArg(1).validateTypes(this, false);
        final Class<? extends GenericValue> type3 = this.getArg(2).validateTypes(this, false);

        if (HUtil.isParentClass(StringValue.class, type1, type2, type3))
            this.typedExpr = new StringBetweenStmt(this.getArg(0), this.isNot(), this.getArg(1), this.getArg(2));
        else if (HUtil.isParentClass(NumberValue.class, type1, type2, type3))
            this.typedExpr = new NumberBetweenStmt(this.getArg(0), this.isNot(), this.getArg(1), this.getArg(2));
        else if (HUtil.isParentClass(DateValue.class, type1, type2, type3))
            this.typedExpr = new DateBetweenStmt(this.getArg(0), this.isNot(), this.getArg(1), this.getArg(2));
        else
            HUtil.throwInvalidTypeException(this, type1, type2, type3);

        return this.typedExpr.validateTypes(parentExpr, false);
    }

    @Override
    public GenericValue getOptimizedValue() throws HBqlException {
        return this.typedExpr.getOptimizedValue();
    }

    @Override
    public Boolean getValue(final Object object) throws HBqlException {
        return this.typedExpr.getValue(object);
    }
}