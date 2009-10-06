package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.hbql.query.antlr.HBql;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.DateLiteral;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.stmt.args.QueryArgs;
import org.apache.hadoop.hbase.hbql.query.stmt.args.WhereArgs;
import org.apache.hadoop.hbase.hbql.query.stmt.select.RowRequest;
import org.apache.hadoop.hbase.hbql.query.util.Lists;
import org.apache.hadoop.hbase.hbql.query.util.Sets;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 9:26:38 PM
 */
public class HQuery<T> {

    private final HConnection connection;
    private final String query;
    private final QueryArgs queryArgs;
    private final List<RowRequest> rowRequestList;

    private List<HQueryListener<T>> listeners = null;

    HQuery(final HConnection connection, final String query) throws IOException, HBqlException {

        this.connection = connection;
        this.query = query;
        this.queryArgs = HBql.parseSelectStmt(this.getConnection(), this.getQuery());

        final WhereArgs where = this.getQueryArgs().getWhereArgs();

        where.setSchema(this.getQueryArgs().getSchema());

        // Get list of all columns that are used in select list and expr tree
        final Set<ColumnAttrib> allAttribs = Sets.newHashSet();
        allAttribs.addAll(this.getQueryArgs().getSelectAttribList());
        allAttribs.addAll(where.getAllColumnsUsedInExprs());

        this.rowRequestList = where.getRowRequestList(allAttribs);
    }

    public synchronized void addListener(final HQueryListener<T> listener) {
        if (this.getListeners() == null)
            this.listeners = Lists.newArrayList();

        this.getListeners().add(listener);
    }

    private HConnection getConnection() {
        return this.connection;
    }

    public String getQuery() {
        return this.query;
    }

    private QueryArgs getQueryArgs() {
        return this.queryArgs;
    }

    private List<RowRequest> getRowRequestList() {
        return this.rowRequestList;
    }

    private List<HQueryListener<T>> getListeners() {
        return this.listeners;
    }

    public void setParameter(final String name, final Object val) throws HBqlException {
        this.getQueryArgs().setParameter(name, val);
    }

    public void clearListeners() {
        if (this.getListeners() != null)
            this.getListeners().clear();
    }

    public HResults<T> getResults() throws HBqlException {

        // Set it once per evaluation
        DateLiteral.resetNow();

        if (this.getListeners() != null) {
            for (final HQueryListener<T> listener : this.getListeners())
                listener.onQueryInit();
        }

        return new HResults<T>(this,
                               this.getConnection(),
                               this.getQueryArgs(),
                               this.getListeners(),
                               this.getRowRequestList());
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
