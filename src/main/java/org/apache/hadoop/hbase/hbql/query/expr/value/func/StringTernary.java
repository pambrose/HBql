package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.PredicateExpr;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.StringLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class StringTernary extends GenericTernary<StringValue> implements StringValue {


    public StringTernary(final PredicateExpr pred, final StringValue expr1, final StringValue expr2) {
        super(pred, expr1, expr2);
    }

    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {

        boolean retval = true;

        if (this.getPred().optimizeForConstants(object))
            this.setPred(new BooleanLiteral(this.getPred().evaluate(object)));
        else
            retval = false;

        if (this.getExpr1().optimizeForConstants(object))
            this.setExpr1(new StringLiteral(this.getExpr1().getValue(object)));
        else
            retval = false;

        if (this.getExpr2().optimizeForConstants(object))
            this.setExpr2(new StringLiteral(this.getExpr2().getValue(object)));
        else
            retval = false;

        return retval;
    }

    @Override
    public String getValue(final Object object) throws HPersistException {
        return (String)super.getValue(object);
    }
}