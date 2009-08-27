package com.imap4j.hbase.hql.expr;

import com.imap4j.hbase.hql.ClassSchema;
import com.imap4j.hbase.hql.HPersistException;
import com.imap4j.hbase.hql.HPersistable;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class BetweenExpr implements ConditionExpr {

    private final ExprType type;
    private final AttribRef attrib;
    private final boolean not;
    private final Object lowerVal, upperVal;

    public BetweenExpr(final ExprType type, final AttribRef attrib, final boolean not, final Object lowerVal, final Object upperVal) {
        this.type = type;
        this.attrib = attrib;
        this.not = not;
        this.lowerVal = lowerVal;
        this.upperVal = upperVal;
    }

    @Override
    public boolean evaluate(final ClassSchema classSchema, final HPersistable recordObj) throws HPersistException {

        switch (type) {
            case IntegerType: {
                final Number objVal = (Number)this.attrib.getValue(classSchema, recordObj);
                final int attribVal = objVal.intValue();
                return attribVal >= ((Number)lowerVal).intValue() && attribVal <= ((Number)upperVal).intValue();
            }

            case StringType: {
                final String attribVal = (String)this.attrib.getValue(classSchema, recordObj);
                // TODO Check this
                return attribVal.compareTo((String)lowerVal) <= 0 && attribVal.compareTo((String)lowerVal) >= 0;
            }
        }

        throw new HPersistException("Unknown type in InExpr.evaluate() - " + type);
    }

}