package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 2:00:25 PM
 */
public abstract class GenericBetweenStmt extends GenericNotValue {

    protected GenericBetweenStmt(final Type type,
                                 final boolean not,
                                 final GenericValue arg0,
                                 final GenericValue arg1,
                                 final GenericValue arg2) {
        super(type, not, arg0, arg1, arg2);
    }

    @Override
    public String asString() {
        return this.getArg(0).asString() + notAsString() + " BETWEEN "
               + this.getArg(1).asString() + " AND " + this.getArg(2).asString();
    }
}
