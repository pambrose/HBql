package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class DelegateTernary extends GenericTernary {

    private GenericTernary typedExpr = null;

    public DelegateTernary(final GenericValue arg0, final GenericValue arg1, final GenericValue arg2) {
        super(null, arg0, arg1, arg2);
    }

    @Override
    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {

        this.validateParentClass(BooleanValue.class, this.getArg(0).validateTypes(this, false));

        final Class<? extends GenericValue> type1 = this.getArg(1).validateTypes(this, false);
        final Class<? extends GenericValue> type2 = this.getArg(2).validateTypes(this, false);

        if (HUtil.isParentClass(StringValue.class, type1, type2))
            this.typedExpr = new StringTernary(this.getArg(0), this.getArg(1), this.getArg(2));
        else if (HUtil.isParentClass(NumberValue.class, type1, type2))
            this.typedExpr = new NumberTernary(this.getArg(0), this.getArg(1), this.getArg(2));
        else if (HUtil.isParentClass(DateValue.class, type1, type2))
            this.typedExpr = new DateTernary(this.getArg(0), this.getArg(1), this.getArg(2));
        else if (HUtil.isParentClass(BooleanValue.class, type1, type2))
            this.typedExpr = new BooleanTernary(this.getArg(0), this.getArg(1), this.getArg(2));
        else
            HUtil.throwInvalidTypeException(this, type1, type2);

        return this.typedExpr.validateTypes(parentExpr, false);
    }

    @Override
    public GenericValue getOptimizedValue() throws HBqlException {
        return this.typedExpr.getOptimizedValue();
    }

    @Override
    public Object getValue(final Object object) throws HBqlException {
        return this.typedExpr.getValue(object);
    }

}