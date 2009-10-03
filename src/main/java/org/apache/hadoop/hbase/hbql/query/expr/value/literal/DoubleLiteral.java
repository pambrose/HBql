package org.apache.hadoop.hbase.hbql.query.expr.value.literal;

import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DoubleValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class DoubleLiteral extends GenericLiteral implements DoubleValue {

    private final Double value;

    public DoubleLiteral(final Double value) {
        this.value = value;
    }

    @Override
    public Double getValue(final Object object) {
        return this.value;
    }

    @Override
    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {
        return DoubleValue.class;
    }

    @Override
    public String asString() {
        return "" + this.value;
    }
}