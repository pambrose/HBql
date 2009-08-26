package com.imap4j.hbase.hql.expr;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class StringInExpr extends InExpr implements Evaluatable {

    private final List<String> strList;

    public StringInExpr(final String attrib, final boolean not, final List<String> strList) {
        super(attrib, not);
        this.strList = strList;
    }

    @Override
    public boolean evaluate() {
        return false;
    }
}