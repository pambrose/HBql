package com.imap4j.hbase.hbql.expr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 10:34:29 AM
 */
public class ReflectionEvalContext extends EvalContext {

    final Object object;

    public ReflectionEvalContext(final Object object) {
        this.object = object;
    }

    public Object getObject() {
        return this.object;
    }
}