package com.imap4j.hbase.hbql.expr;

import com.imap4j.hbase.hbase.HPersistable;
import com.imap4j.hbase.hbql.schema.ExprSchema;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 10:34:29 AM
 */
public class HBqlEvalContext extends EvalContext {

    final HPersistable recordObj;

    public HBqlEvalContext(final ExprSchema exprSchema, final HPersistable recordObj) {
        super(exprSchema);
        this.recordObj = recordObj;
    }

    public HPersistable getObject() {
        return this.recordObj;
    }
}