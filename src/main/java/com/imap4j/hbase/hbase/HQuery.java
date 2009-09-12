package com.imap4j.hbase.hbase;

import com.imap4j.hbase.hbql.expr.ExprTree;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.schema.AnnotationSchema;
import com.imap4j.hbase.util.Lists;

import java.io.IOException;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 9:26:38 PM
 */
public class HQuery<T extends HPersistable> {

    final String query;
    final List<HQueryListener<T>> listeners = Lists.newArrayList();
    ;

    private HQuery(final String query) {
        this.query = query;
    }

    public static <T extends HPersistable> HQuery<T> newHQuery(final String query) {
        return new HQuery<T>(query);
    }

    public void addListener(final HQueryListener<T> listener) {
        this.getListeners().add(listener);
    }

    public String getQuery() {
        return this.query;
    }

    List<HQueryListener<T>> getListeners() {
        return this.listeners;
    }

    ExprTree getExprTree(final ExprTree exprTree,
                         final AnnotationSchema schema,
                         final List<String> fieldList) throws HPersistException {

        if (exprTree != null) {
            exprTree.setSchema(schema);
            exprTree.optimize();

            // Check if all the variables referenced in the where clause are present in the fieldList.
            final List<ExprVariable> vars = exprTree.getExprVariables();
            for (final ExprVariable var : vars) {
                if (!fieldList.contains(var.getName()))
                    throw new HPersistException("Variable " + var.getName() + " used in where clause but it is not "
                                                + "not in the select list");
            }
        }

        return exprTree;
    }

    public HResults<T> execute() throws IOException, HPersistException {

        final HResults<T> retval = new HResults<T>(this);

        if (this.getListeners().size() > 0) {
            try {
                for (final HQueryListener<T> listener : this.getListeners())
                    listener.onQueryInit();

                for (final T val : retval)
                    for (final HQueryListener<T> listener : this.getListeners())
                        listener.onEachRow((T)val);

                for (final HQueryListener<T> listener : this.getListeners())
                    listener.onQueryComplete();
            }
            finally {
                retval.close();
            }
        }

        return retval;
    }
}
