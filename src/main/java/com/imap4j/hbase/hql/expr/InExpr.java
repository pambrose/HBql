package com.imap4j.hbase.hql.expr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class InExpr implements Evaluatable {

    public String attrib;
    public boolean not;

    @Override
    public boolean evaluate() {
        return false;
    }
}