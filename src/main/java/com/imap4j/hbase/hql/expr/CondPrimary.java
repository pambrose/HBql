package com.imap4j.hbase.hql.expr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 8:28:42 PM
 */
public class CondPrimary implements Evaluatable {
    public Evaluatable expr;

    @Override
    public boolean evaluate() {
        return false;
    }
}
