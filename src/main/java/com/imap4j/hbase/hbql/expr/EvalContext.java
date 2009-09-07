package com.imap4j.hbase.hbql.expr;

import com.imap4j.hbase.hbql.HPersistable;
import com.imap4j.hbase.hbql.schema.ClassSchema;

import java.io.Serializable;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 10:34:29 AM
 */
public class EvalContext implements Serializable {

    final ClassSchema classSchema;
    final HPersistable recordObj;

    public EvalContext(final ClassSchema classSchema, final HPersistable recordObj) {
        this.classSchema = classSchema;
        this.recordObj = recordObj;
    }

    public ClassSchema getClassSchema() {
        return this.classSchema;
    }

    public HPersistable getRecordObj() {
        return this.recordObj;
    }
}
