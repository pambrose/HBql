package com.imap4j.hbase.hbql.expr;

import com.imap4j.hbase.hbql.HPersistable;
import com.imap4j.hbase.hbql.schema.ClassSchema;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 10:34:29 AM
 */
public class AttribContext {

    final ClassSchema classSchema;
    final HPersistable recordObj;

    public AttribContext(final ClassSchema classSchema, final HPersistable recordObj) {
        this.classSchema = classSchema;
        this.recordObj = recordObj;
    }

    public ClassSchema getClassSchema() {
        return classSchema;
    }

    public HPersistable getRecordObj() {
        return recordObj;
    }
}
