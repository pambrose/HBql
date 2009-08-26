package com.imap4j.hbase.hql.expr;

import com.imap4j.hbase.hql.ClassSchema;
import com.imap4j.hbase.hql.HPersistException;
import com.imap4j.hbase.hql.HPersistable;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 8:23:42 PM
 */
public class AndExpr implements Evaluatable {
    public CondFactor expr1;
    public AndExpr expr2;

    @Override
    public boolean evaluate(final ClassSchema classSchema, final HPersistable recordObj) throws HPersistException {
        if (expr2 == null)
            return expr1.evaluate(classSchema, recordObj);
        else
            return expr1.evaluate(classSchema, recordObj) && expr2.evaluate(classSchema, recordObj);
    }
}
