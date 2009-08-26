package com.imap4j.hbase.hql.expr;

import com.imap4j.hbase.hql.ClassSchema;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class StringLiteral implements Evaluatable {

    private final String value;

    public StringLiteral(final String attrib, final boolean not, final List<String> strList) {
        super(attrib, not);
        this.strList = strList;
    }

    @Override
    public boolean evaluate(final ClassSchema classSchema, final Object recordObj) {
        return false;
    }
}