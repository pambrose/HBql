package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 5:28:17 PM
 */
public class NumberFunction extends GenericFunction<NumberValue> implements NumberValue {

    public NumberFunction(final FunctionType functionType, final StringValue... stringExprs) {
        super(functionType, stringExprs);
    }

    @Override
    public Class<? extends ValueExpr> validateType() throws HPersistException {
        switch (this.getFunctionType()) {
            case LENGTH: {
                final Class<? extends ValueExpr> type1 = this.getStringExprs()[0].validateType();
                if (!ExprTree.isOfType(type1, StringValue.class))
                    throw new HPersistException("Type " + type1.getName() + " not valid in LENGTH");
                break;
            }

            case INDEXOF: {
                final Class<? extends ValueExpr> type1 = this.getStringExprs()[0].validateType();
                final Class<? extends ValueExpr> type2 = this.getStringExprs()[1].validateType();
                if (!ExprTree.isOfType(type1, StringValue.class))
                    throw new HPersistException("Type " + type1.getName() + " not valid in INDEXOF");
                if (!ExprTree.isOfType(type2, StringValue.class))
                    throw new HPersistException("Type " + type2.getName() + " not valid in INDEXOF");
                break;
            }

            default:
                throw new HPersistException("Error in NumberFunction.validateType() " + this.getFunctionType());
        }

        return NumberValue.class;
    }

    @Override
    public Number getValue(final Object object) throws HPersistException {

        switch (this.getFunctionType()) {
            case LENGTH: {
                final String val = this.getStringExprs()[0].getValue(object);
                if (val == null)
                    return 0;
                else
                    return val.length();
            }

            case INDEXOF: {
                final String val1 = this.getStringExprs()[0].getValue(object);
                final String val2 = this.getStringExprs()[1].getValue(object);
                if (val1 == null || val2 == null)
                    return -1;
                else
                    return val1.indexOf(val2);
            }

            default:
                throw new HPersistException("Error in NumberFunction.getValue() " + this.getFunctionType());
        }

    }

}