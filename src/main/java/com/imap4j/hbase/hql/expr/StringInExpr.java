package com.imap4j.hbase.hql.expr;

import com.imap4j.hbase.hql.ClassSchema;
import com.imap4j.hbase.hql.HPersistException;
import com.imap4j.hbase.hql.HPersistable;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class StringInExpr extends InExpr implements Evaluatable {

    private final List<String> strList;

    public StringInExpr(final AttribRef attrib, final boolean not, final List<String> strList) {
        super(attrib, not);
        this.strList = strList;
    }

    @Override
    public boolean evaluate(final ClassSchema classSchema, final HPersistable recordObj) throws HPersistException {
        final String attribVal = (String)this.attrib.getValue(classSchema, recordObj);
        for (final String val : strList)
            if (attribVal.equals(val))
                return true;
        return false;
    }
}