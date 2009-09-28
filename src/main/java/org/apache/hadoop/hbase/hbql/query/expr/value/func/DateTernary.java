package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.DateConstant;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class DateTernary extends GenericTernary implements DateValue {

    public DateTernary(final GenericValue arg0, final GenericValue arg1, final GenericValue arg2) {
        super(arg0, arg1, arg2);
    }

    @Override
    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {
        return this.validateType(DateValue.class);
    }

    @Override
    public GenericValue getOptimizedValue() throws HBqlException {

        this.setArg(0, this.getArg(0).getOptimizedValue());
        this.setArg(1, this.getArg(1).getOptimizedValue());
        this.setArg(2, this.getArg(2).getOptimizedValue());

        return this.isAConstant() ? new DateConstant(this.getValue(null)) : this;
    }

    @Override
    public Long getValue(final Object object) throws HBqlException {
        return (Long)super.getValue(object);
    }
}