package com.imap4j.hbase.hbql.expr;

import com.imap4j.hbase.hbql.schema.ExprSchema;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 10:34:29 AM
 */
public class ReflectionEvalContext extends EvalContext {

    final Object object;

    public ReflectionEvalContext(final ExprSchema exprSchema, final Object object) {
        super(exprSchema);
        this.object = object;
    }

    public ExprSchema getExprSchema() {
        return this.exprSchema;
    }

    public Object getObject() {
        return this.object;
    }
}