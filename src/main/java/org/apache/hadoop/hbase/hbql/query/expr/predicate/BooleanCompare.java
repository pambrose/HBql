package org.apache.hadoop.hbase.hbql.query.expr.predicate;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.func.Operator;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class BooleanCompare extends GenericCompare implements BooleanValue {

    public BooleanCompare(final GenericValue arg0, final Operator operator, final GenericValue arg1) {
        super(arg0, operator, arg1);
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {
        return this.validateType(BooleanValue.class);
    }

    @Override
    public Boolean getValue(final Object object) throws HBqlException {

        final boolean expr1val = (Boolean)this.getArg(0).getValue(object);
        final boolean expr2val = (Boolean)this.getArg(1).getValue(object);

        switch (this.getOperator()) {
            case OR:
                return expr1val || expr2val;
            case AND:
                return expr1val && expr2val;
            case EQ:
                return expr1val == expr2val;
            case NOTEQ:
                return expr1val != expr2val;
            default:
                throw new HBqlException("Invalid operator: " + this.getOperator());
        }
    }
}
