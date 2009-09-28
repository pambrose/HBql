package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.GenericExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 1:51:03 PM
 */
public abstract class GenericTernary extends GenericExpr {

    protected GenericTernary(final GenericValue arg0, final GenericValue arg1, final GenericValue arg2) {
        super(null, arg0, arg1, arg2);
    }

    @Override
    public Object getValue(final Object object) throws HBqlException {
        if ((Boolean)this.getArg(0).getValue(object))
            return this.getArg(1).getValue(object);
        else
            return this.getArg(2).getValue(object);
    }

    protected Class<? extends GenericValue> validateType(final Class<? extends GenericValue> clazz) throws TypeException {
        this.validateParentClass(BooleanValue.class, this.getArg(0).validateTypes(this, false));
        this.validateParentClass(clazz,
                                 this.getArg(1).validateTypes(this, false),
                                 this.getArg(2).validateTypes(this, false));
        return clazz;
    }

    @Override
    public String asString() {
        return "IF " + this.getArg(0).asString() + " THEN "
               + this.getArg(1).asString()
               + " ELSE " + this.getArg(2).asString() + " END";
    }

}
