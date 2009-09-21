package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 2:00:25 PM
 */
public abstract class GenericNotValue implements BooleanValue {

    private final boolean not;

    protected GenericNotValue(final boolean not) {
        this.not = not;
    }

    public boolean isNot() {
        return not;
    }

}