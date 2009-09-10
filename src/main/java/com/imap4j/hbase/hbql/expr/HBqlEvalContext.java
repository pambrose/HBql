package com.imap4j.hbase.hbql.expr;

import com.imap4j.hbase.hbase.HPersistable;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 10:34:29 AM
 */
public class HBqlEvalContext extends EvalContext {

    final HPersistable recordObj;

    public HBqlEvalContext(final HPersistable recordObj) {
        this.recordObj = recordObj;
    }

    public HPersistable getObject() {
        return this.recordObj;
    }
}