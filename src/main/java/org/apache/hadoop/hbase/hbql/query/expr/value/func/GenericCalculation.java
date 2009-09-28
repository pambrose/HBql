package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.GenericExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.TypeSignature;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 7, 2009
 * Time: 9:29:44 PM
 */
public abstract class GenericCalculation extends GenericExpr {

    private final Operator operator;

    protected GenericCalculation(final TypeSignature typeSignature,
                                 final GenericValue arg0,
                                 final Operator operator,
                                 final GenericValue arg1) {
        super(typeSignature, arg0, arg1);
        this.operator = operator;
    }

    protected Operator getOperator() {
        return this.operator;
    }

    @Override
    public String asString() {
        if (this.getOperator() == Operator.NEGATIVE)
            return "-" + this.getArg(0).asString();
        else
            return this.getArg(0).asString() + " " + this.getOperator() + " " + this.getArg(1).asString();
    }
}
