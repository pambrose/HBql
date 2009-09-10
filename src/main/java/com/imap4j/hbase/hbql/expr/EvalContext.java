package com.imap4j.hbase.hbql.expr;

import com.imap4j.hbase.hbql.schema.ExprSchema;

import java.io.Serializable;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 10:34:29 AM
 */
public class EvalContext implements Serializable {

    private final Object object;
    private ExprSchema exprSchema;

    public EvalContext(final Object object) {
        this.object = object;
    }

    public void setExprSchema(final ExprSchema exprSchema) {
        this.exprSchema = exprSchema;
    }

    public ExprSchema getExprSchema() {
        return this.exprSchema;
    }

    public Object getObject() {
        return this.object;
    }
}
