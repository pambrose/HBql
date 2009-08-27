package com.imap4j.hbase.hql.expr;

import com.imap4j.hbase.hql.ClassSchema;
import com.imap4j.hbase.hql.HPersistException;
import com.imap4j.hbase.hql.HPersistable;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 8:28:06 PM
 */
public class CondFactor implements ConditionExpr {

    private final boolean not;
    private final CondPrimary primary;

    public CondFactor(final boolean not, final CondPrimary primary) {
        this.not = not;
        this.primary = primary;
    }

    @Override
    public boolean evaluate(final ClassSchema classSchema, final HPersistable recordObj) throws HPersistException {
        if (this.not)
            return !this.primary.evaluate(classSchema, recordObj);
        else
            return this.primary.evaluate(classSchema, recordObj);
    }
}
