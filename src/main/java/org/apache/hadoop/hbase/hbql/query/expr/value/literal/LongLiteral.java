package org.apache.hadoop.hbase.hbql.query.expr.value.literal;

import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class LongLiteral extends GenericLiteral<NumberValue> implements NumberValue {

    private final Long value;

    public LongLiteral(final Long value) {
        this.value = value;
    }

    @Override
    public Long getValue(final Object object) {
        return this.value;
    }
}