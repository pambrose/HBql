package org.apache.hadoop.hbase.hbql.query.impl.hbase;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HQuery;
import org.apache.hadoop.hbase.hbql.client.HQueryListener;
import org.apache.hadoop.hbase.hbql.client.HResults;
import org.apache.hadoop.hbase.hbql.query.antlr.HBql;
import org.apache.hadoop.hbase.hbql.query.expr.literal.DateLiteral;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.stmt.args.SelectStmt;
import org.apache.hadoop.hbase.hbql.query.stmt.args.WhereArgs;
import org.apache.hadoop.hbase.hbql.query.stmt.select.RowRequest;
import org.apache.hadoop.hbase.hbql.query.util.Lists;
import org.apache.hadoop.hbase.hbql.query.util.Sets;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class QueryImpl<T> implements HQuery<T> {

    private final HConnection connection;
    private final String query;
    private final SelectStmt selectStmt;

    private List<HQueryListener<T>> listeners = null;

    public QueryImpl(final HConnection connection, final String query) throws HBqlException {

        this.connection = connection;
        this.query = query;
        this.selectStmt = HBql.parseSelectStmt(this.getConnection(), this.getQuery());

        this.getQueryArgs().getWhereArgs().setSchema(this.getQueryArgs().getSchema());
    }

    public synchronized void addListener(final HQueryListener<T> listener) {
        if (this.getListeners() == null)
            this.listeners = Lists.newArrayList();

        this.getListeners().add(listener);
    }

    public HConnection getConnection() {
        return this.connection;
    }

    public String getQuery() {
        return this.query;
    }

    public SelectStmt getQueryArgs() {
        return this.selectStmt;
    }

    public List<RowRequest> getRowRequestList() throws HBqlException, IOException {

        final WhereArgs where = this.getQueryArgs().getWhereArgs();

        // Get list of all columns that are used in select list and expr tree
        final Set<ColumnAttrib> allAttribs = Sets.newHashSet();
        allAttribs.addAll(this.getQueryArgs().getSelectAttribList());
        allAttribs.addAll(where.getAllColumnsUsedInExprs());

        return where.getRowRequestList(allAttribs);
    }

    public List<HQueryListener<T>> getListeners() {
        return this.listeners;
    }

    public void setParameter(final String name, final Object val) throws HBqlException {
        int cnt = this.getQueryArgs().setParameter(name, val);
        if (cnt == 0)
            throw new HBqlException("Parameter name " + name + " does not exist in " + this.getQuery());
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
