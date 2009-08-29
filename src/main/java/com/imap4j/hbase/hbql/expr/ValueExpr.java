package com.imap4j.hbase.hbql.expr;

import com.imap4j.hbase.hbql.ClassSchema;
import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.HPersistable;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 26, 2009
 * Time: 10:18:22 AM
 */
public interface ValueExpr {

    Object getValue(ClassSchema classSchema, HPersistable recordObj) throws HPersistException;
}
