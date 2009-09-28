package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public abstract class GenericNullCompare extends GenericNotValue {

    protected GenericNullCompare(final Type type, final boolean not, final GenericValue arg0) {
        super(type, not, arg0);
    }

    @Override
    public String asString() {
        return this.getArg(0).asString() + " IS" + notAsString() + " NULL";
    }

}