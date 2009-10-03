package org.apache.hadoop.hbase.hbql.query.expr.value.literal;

import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.FloatValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class FloatLiteral extends GenericLiteral implements FloatValue {

    private final Float value;

    public FloatLiteral(final Float value) {
        this.value = value;
    }

    @Override
    public Float getValue(final Object object) {
        return this.value;
    }

    @Override
    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {
        return FloatValue.class;
    }

    @Override
    public String asString() {
        return "" + this.value;
    }
}