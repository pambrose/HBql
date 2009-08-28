package com.imap4j.hbase.hql.expr;

import com.imap4j.hbase.hql.ClassSchema;
import com.imap4j.hbase.hql.HPersistException;
import com.imap4j.hbase.hql.HPersistable;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class OrExpr implements PredicateExpr {

    private final PredicateExpr expr1;
    private final PredicateExpr expr2;

    public OrExpr(final PredicateExpr expr1, final PredicateExpr expr2) {
        this.expr1 = expr1;
        this.expr2 = expr2;
    }

    @Override
    public boolean evaluate(final ClassSchema classSchema, final HPersistable recordObj) throws HPersistException {
        if (expr2 == null)
            return expr1.evaluate(classSchema, recordObj);
        else
            return expr1.evaluate(classSchema, recordObj) || expr2.evaluate(classSchema, recordObj);
    }
}
