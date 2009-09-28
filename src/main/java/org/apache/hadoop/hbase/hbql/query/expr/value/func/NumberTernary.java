package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.NumberLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class NumberTernary extends GenericTernary implements NumberValue {

    public NumberTernary(final GenericValue arg0, final GenericValue arg1, final GenericValue arg2) {
        super(arg0, arg1, arg2);
    }

    @Override
    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {
        return this.validateType(NumberValue.class);
    }

    @Override
    public GenericValue getOptimizedValue() throws HBqlException {

        this.setArg(0, this.getArg(0).getOptimizedValue());
        this.setArg(1, this.getArg(1).getOptimizedValue());
        this.setArg(2, this.getArg(2).getOptimizedValue());

        return this.isAConstant() ? new NumberLiteral(this.getValue(null)) : this;
    }

    @Override
    public Number getValue(final Object object) throws HBqlException {
        return (Number)super.getValue(object);
    }
}
