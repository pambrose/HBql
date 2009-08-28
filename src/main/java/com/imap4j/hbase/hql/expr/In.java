package com.imap4j.hbase.hql.expr;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
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
public class In implements PredicateExpr {


    private final ExprType type;
    private final ValueExpr expr;
    private final boolean not;
    private final List<Object> valList;

    public In(final ExprType type, final ValueExpr expr, final boolean not, final List<Object> valList) {
        this.type = type;
        this.expr = expr;
        this.not = not;
        this.valList = valList;
    }

    @Override
    public boolean evaluate(final ClassSchema classSchema, final HPersistable recordObj) throws HPersistException {

        final boolean retval;

        switch (type) {

            case IntegerType: {
                final Number attribVal = (Number)this.expr.getValue(classSchema, recordObj);
                final int intAttrib = attribVal.intValue();

                retval = Iterables.any(this.valList,
                                       new Predicate<Object>() {
                                           @Override
                                           public boolean apply(final Object obj) {
                                               final Number numobj = (Number)obj;
                                               final int val = numobj.intValue();
                                               return val == intAttrib;
                                           }
                                       });
                break;

            }

            case StringType: {
                final String attribVal = (String)this.expr.getValue(classSchema, recordObj);
                retval = Iterables.any(this.valList,
                                       new Predicate<Object>() {
                                           @Override
                                           public boolean apply(final Object obj) {
                                               return attribVal.equals(obj);
                                           }
                                       });
                break;

            }
            default:
                throw new HPersistException("Unknown type in In.evaluate() - " + type);

        }

        return (this.not) ? !retval : retval;
    }

}