package com.imap4j.hbase.hql.expr;

import com.imap4j.hbase.hql.ClassSchema;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 8:28:06 PM
 */
public class CondFactor implements Evaluatable {

    public boolean not;
    public CondPrimary primary;

    @Override
    public boolean evaluate(final ClassSchema classSchema, final Object recordObj) {
        if (this.not)
            return !this.primary.evaluate(nil, nil);
        else
            return this.primary.evaluate(nil, nil);
    }
}
