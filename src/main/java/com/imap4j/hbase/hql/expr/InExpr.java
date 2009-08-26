package com.imap4j.hbase.hql.expr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public abstract class InExpr implements Evaluatable {

    protected final AttribRef attrib;
    protected final boolean not;

    protected InExpr(final AttribRef attrib, final boolean not) {
        this.attrib = attrib;
        this.not = not;
    }
}