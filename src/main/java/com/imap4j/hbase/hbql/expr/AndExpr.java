package com.imap4j.hbase.hbql.expr;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.HPersistable;
import com.imap4j.hbase.hbql.schema.ClassSchema;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 8:23:42 PM
 */
public class AndExpr implements PredicateExpr {

    private final PredicateExpr expr1;
    private final PredicateExpr expr2;

    public AndExpr(final PredicateExpr expr1, final PredicateExpr expr2) {
        this.expr1 = expr1;
        this.expr2 = expr2;
    }

    @Override
    public boolean evaluate(final ClassSchema classSchema, final HPersistable recordObj) throws HPersistException {
        if (this.expr2 == null)
            return this.expr1.evaluate(classSchema, recordObj);
        else
            return this.expr1.evaluate(classSchema, recordObj) && this.expr2.evaluate(classSchema, recordObj);
    }
}
