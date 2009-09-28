package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public abstract class GenericNullCompare extends GenericNotValue {

    protected GenericNullCompare(final boolean not, final GenericValue arg0) {
        super(not, arg0);
    }

    protected Class<? extends GenericValue> validateType(final Class<? extends GenericValue> clazz) throws TypeException {
        this.validateParentClass(clazz, this.getArg(0).validateTypes(this, false));
        return BooleanValue.class;
    }

    @Override
    public String asString() {
        return this.getArg(0).asString() + " IS" + notAsString() + " NULL";
    }

}