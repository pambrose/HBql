package com.imap4j.hbase.hql.expr;

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
    public boolean evaluate() {
        return false;
    }
}
