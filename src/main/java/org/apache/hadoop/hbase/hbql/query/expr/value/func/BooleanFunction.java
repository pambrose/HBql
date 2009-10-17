package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

public class BooleanFunction extends Function implements BooleanValue {


    public BooleanFunction(final Type functionType, final GenericValue... exprs) {
        super(functionType, exprs);
    }


    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {

        switch (this.getFunctionType()) {

            case VALID: {
                try {
                    this.getArg(0).getValue(object);
                    return true;
                }
                catch (ResultMissingColumnException e) {
                    return false;
                }
            }

            default:
                throw new HBqlException("Invalid function: " + this.getFunctionType());
        }
    }
}