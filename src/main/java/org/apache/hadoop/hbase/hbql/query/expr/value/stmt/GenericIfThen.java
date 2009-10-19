package org.apache.hadoop.hbase.hbql.query.expr.value.stmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.GenericExpr;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

public abstract class GenericIfThen extends GenericExpr {

    protected GenericIfThen(final Type type,
                            final GenericValue arg0,
                            final GenericValue arg1,
                            final GenericValue arg2) {
        super(type, arg0, arg1, arg2);
    }

    public Object getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        if ((Boolean)this.getArg(0).getValue(object))
            return this.getArg(1).getValue(object);
        else
            return this.getArg(2).getValue(object);
    }

    public String asString() {
        return "IF " + this.getArg(0).asString() + " THEN "
               + this.getArg(1).asString()
               + " ELSE " + this.getArg(2).asString() + " END";
    }
}
