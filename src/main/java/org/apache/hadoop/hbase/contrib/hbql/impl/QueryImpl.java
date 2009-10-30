package org.apache.hadoop.hbase.contrib.hbql.impl;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.expr.literal.DateLiteral;
import org.apache.expreval.util.Lists;
import org.apache.expreval.util.Sets;
import org.apache.hadoop.hbase.contrib.hbql.client.HConnection;
import org.apache.hadoop.hbase.contrib.hbql.client.HQuery;
import org.apache.hadoop.hbase.contrib.hbql.client.HQueryListener;
import org.apache.hadoop.hbase.contrib.hbql.client.HResults;
import org.apache.hadoop.hbase.contrib.hbql.parser.HBqlParser;
import org.apache.hadoop.hbase.contrib.hbql.schema.ColumnAttrib;
import org.apache.hadoop.hbase.contrib.hbql.statement.SelectStatement;
import org.apache.hadoop.hbase.contrib.hbql.statement.args.WithArgs;
import org.apache.hadoop.hbase.contrib.hbql.statement.select.RowRequest;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class QueryImpl<T> implements HQuery<T> {

    private final HConnection connection;
    private final SelectStatement selectStatement;

    private List<HQueryListener<T>> listeners = null;

    public QueryImpl(final HConnection connection, final SelectStatement selectStatement) throws HBqlException {
        this.connection = connection;
        this.selectStatement = selectStatement;
    }

    public QueryImpl(final HConnection connection, final String query) throws HBqlException {
        this(connection, HBqlParser.parseSelectStatement(connection, query));
    }

    public synchronized void addListener(final HQueryListener<T> listener) {
        if (this.getListeners() == null)
            this.listeners = Lists.newArrayList();

        this.getListeners().add(listener);
    }

    public HConnection getConnection() {
        return this.connection;
    }

    public SelectStatement getSelectStatement() {
        return this.selectStatement;
    }

    public List<RowRequest> getRowRequestList() throws HBqlException, IOException {

        final WithArgs with = this.getSelectStatement().getWithArgs();

        // Get list of all columns that are used in select list and expr tree
        final Set<ColumnAttrib> allAttribs = Sets.newHashSet();
        allAttribs.addAll(this.getSelectStatement().getSelectAttribList());
        allAttribs.addAll(with.getAllColumnsUsedInExprs());

        return with.getRowRequestList(allAttribs);
    }

    public List<HQueryListener<T>> getListeners() {
        return this.listeners;
    }

    public void setParameter(final String name, final Object val) throws HBqlException {
        int cnt = this.getSelectStatement().setParameter(name, val);
        if (cnt == 0)
            throw new HBqlException("Parameter name " + name + " does not exist in "
                                    + this.getSelectStatement().asString());
    }

    public void clearListeners() {
        if (this.getListeners() != null)
            this.getListeners().clear();
    }

    public HResults<T> getResults() {

        // Set it once per evaluation
        DateLiteral.resetNow();

        if (this.getListeners() != null) {
            for (final HQueryListener<T> listener : this.getListeners())
                listener.onQueryInit();
        }

        return new ResultsImpl<T>(this);
    }

    public List<T> getResultList() throws HBqlException {

        final List<T> retval = Lists.newArrayList();

        HResults<T> results = null;

        try {
            results = this.getResults();

            for (T val : results)
                retval.add(val);
        }
        finally {
            if (results != null)
                results.close();
        }

        return retval;
    }
}
