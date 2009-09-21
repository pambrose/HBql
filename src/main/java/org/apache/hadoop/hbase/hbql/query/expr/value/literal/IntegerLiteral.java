package org.apache.hadoop.hbase.hbql.query.expr.value.literal;

import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class IntegerLiteral extends GenericLiteral implements NumberValue {

    private final Integer value;

    public IntegerLiteral(final Integer value) {
        this.value = value;
    }

    @Override
    public Integer getValue(final Object object) {
        return this.value;
    }
}