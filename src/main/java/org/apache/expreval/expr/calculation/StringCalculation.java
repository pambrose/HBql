package org.apache.expreval.expr.calculation;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.Operator;
import org.apache.expreval.expr.node.GenericValue;

public class StringCalculation extends GenericCalculation {

    public StringCalculation(final GenericValue arg0, final Operator operator, final GenericValue arg1) {
        super(ExpressionType.STRINGCALCULATION, arg0, operator, arg1);
    }

    public String getValue(final Object object) throws HBqlException, ResultMissingColumnException {

        final String val1 = (String)this.getArg(0).getValue(object);
        final String val2 = (String)this.getArg(1).getValue(object);

        switch (this.getOperator()) {
            case PLUS:
                return val1 + val2;
            default:
                throw new HBqlException("Invalid operator: " + this.getOperator());
        }
    }
}