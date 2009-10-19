package org.apache.hadoop.hbase.hbql.query.expr.value.stmt;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.GenericExpr;

import java.util.List;

public abstract class GenericCase extends GenericExpr {

    private final List<GenericCaseWhen> whenExprList;
    private GenericCaseElse elseExpr;

    protected GenericCase(final Type type, final List<GenericCaseWhen> whenExprList, final GenericCaseElse elseExpr) {
        super(type);
        this.whenExprList = whenExprList;
        this.elseExpr = elseExpr;
    }

    public Object getValue(final Object object) throws HBqlException, ResultMissingColumnException {

        for (final GenericCaseWhen when : this.getWhenExprList()) {
            if (when.getPredicateValue(object))
                return when.getValue(object);
        }

        if (this.getElseExpr() != null)
            this.getElseExpr().getValue(object);

        return null;
    }

    public String asString() {

        final StringBuilder sbuf = new StringBuilder();

        sbuf.append("CASE ");

        for (final GenericCaseWhen when : this.getWhenExprList())
            sbuf.append(when.asString());

        if (this.getElseExpr() != null)
            sbuf.append(this.getElseExpr().asString());

        sbuf.append(" END");

        return sbuf.toString();
    }

    protected List<GenericCaseWhen> getWhenExprList() {
        return this.whenExprList;
    }

    protected GenericCaseElse getElseExpr() {
        return this.elseExpr;
    }

    protected void setElseExpr(final GenericCaseElse elseExpr) {
        this.elseExpr = elseExpr;
    }
}