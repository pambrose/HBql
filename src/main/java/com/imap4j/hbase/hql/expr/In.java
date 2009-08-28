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
public class In implements Predicate {


    private final ExprType type;
    private final AttribRef attrib;
    private final boolean not;
    private final List<Object> valList;

    public In(final ExprType type, final AttribRef attrib, final boolean not, final List<Object> valList) {
        this.type = type;
        this.attrib = attrib;
        this.not = not;
        this.valList = valList;
    }

    @Override
    public boolean evaluate(final ClassSchema classSchema, final HPersistable recordObj) throws HPersistException {

        switch (type) {

            case IntegerType: {
                final Number attribVal = (Number)this.attrib.getValue(classSchema, recordObj);
                for (final Object obj : this.valList)
                    if (attribVal.equals(obj))
                        return true;
                return false;

            }

            case StringType: {
                final String attribVal = (String)this.attrib.getValue(classSchema, recordObj);
                for (final Object obj : this.valList)
                    if (attribVal.equals(obj))
                        return true;
                return false;

            }
        }

        throw new HPersistException("Unknown type in InExpr.evaluate() - " + type);
    }

}