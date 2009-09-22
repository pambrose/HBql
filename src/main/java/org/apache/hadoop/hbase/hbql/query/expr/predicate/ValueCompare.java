package org.apache.hadoop.hbase.hbql.query.expr.predicate;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public class ValueCompare extends GenericCompare<ValueExpr> {

    public ValueCompare(final ValueExpr expr1, final OP op, final ValueExpr expr2) {
        super(expr1, op, expr2);
    }

    public Class getType(final Object object) throws HPersistException {

        //  this.setExpr1Type(this.getExpr1().getType(object));
        //  this.setExpr2Type(this.getExpr2().getType(object));

        if (!this.getExpr1Type().equals(getExpr2Type()))
            throw new HPersistException("Types in ValueCompare do not match");

        if (!this.ofType(this.getExpr1Type(), StringValue.class, NumberValue.class, DateValue.class))
            throw new HPersistException("Type " + this.getExpr1Type().getName() + " not allowed in ValueCompare");

        return BooleanValue.class;
    }

    private boolean ofType(final Class clazz, final Class... classes) {
        return true;
    }

    private ValueExpr getLiteral(final ValueExpr expr) {

        //    if (expr.getType().equals(DateValue.class))
        //         return new DateLiteral(expr.getValue(object));
        return null;
    }

    @Override
    public Boolean getValue(final Object object) throws HPersistException {

        /*
        final long val1 = this.getExpr1().getValue(object);
        final long val2 = this.getExpr2().getValue(object);

        switch (this.getOp()) {
            case EQ:
                return val1 == val2;
            case NOTEQ:
                return val1 != val2;
            case GT:
                return val1 > val2;
            case GTEQ:
                return val1 >= val2;
            case LT:
                return val1 < val2;
            case LTEQ:
                return val1 <= val2;
        }
        */
        throw new HPersistException("Error in ValueExprCompare.getValue()");
    }

}