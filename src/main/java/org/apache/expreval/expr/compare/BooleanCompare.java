package org.apache.expreval.expr.compare;

import org.apache.expreval.expr.Operator;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.ResultMissingColumnException;

public class BooleanCompare extends GenericCompare implements BooleanValue {

    public BooleanCompare(final GenericValue arg0, final Operator operator, final GenericValue arg1) {
        super(arg0, operator, arg1);
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws HBqlException {
        return this.validateType(BooleanValue.class);
    }

    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {

        final boolean v1 = (Boolean)this.getArg(0).getValue(object);
        final boolean v2 = (Boolean)this.getArg(1).getValue(object);

        switch (this.getOperator()) {
            case OR:
                return v1 || v2;
            case AND:
                return v1 && v2;
            case EQ:
                return v1 == v2;
            case NOTEQ:
                return v1 != v2;
            default:
                throw new HBqlException("Invalid operator: " + this.getOperator());
        }
    }
}
