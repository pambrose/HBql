package org.apache.hadoop.hbase.hbql.query.expr.value.literal;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.GenericExpr;

public class BooleanExpr extends GenericExpr implements BooleanValue {

    public BooleanExpr(final GenericValue arg0) {
        super(Type.BOOLEANEXPR, arg0);
    }

    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Boolean)this.getArg(0).getValue(object);
    }

    public String asString() {
        return this.getArg(0).asString();
    }
}