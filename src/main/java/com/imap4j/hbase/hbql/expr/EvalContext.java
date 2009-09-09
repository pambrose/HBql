package com.imap4j.hbase.hbql.expr;

import com.imap4j.hbase.hbql.HPersistable;
import com.imap4j.hbase.hbql.schema.ExprSchema;

import java.io.Serializable;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 10:34:29 AM
 */
public class EvalContext implements Serializable {

    final ExprSchema exprSchema;
    final HPersistable recordObj;

    public EvalContext(final ExprSchema exprSchema, final HPersistable recordObj) {
        this.exprSchema = exprSchema;
        this.recordObj = recordObj;
    }

    public ExprSchema getExprSchema() {
        return this.exprSchema;
    }

    public HPersistable getRecordObj() {
        return this.recordObj;
    }
}
