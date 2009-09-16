package com.imap4j.hbase.hbase;

import com.imap4j.hbase.antlr.args.QueryArgs;
import com.imap4j.hbase.antlr.args.WhereArgs;
import com.imap4j.hbase.antlr.config.HBqlRule;
import com.imap4j.hbase.hbql.expr.ExprTree;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.schema.AnnotationSchema;
import com.imap4j.hbase.hbql.schema.ExprSchema;
import com.imap4j.hbase.hbql.schema.HBaseSchema;
import com.imap4j.hbase.hbql.schema.HUtil;
import com.imap4j.hbase.util.Lists;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 9:26:38 PM
 */
public class HQuery<T> {

    final HConnection connection;
    final String query;
    final HBaseSchema schema;
    final List<String> fieldList;
    final ExprTree clientFilter;
    final List<Scan> scanList;

    private List<HQueryListener<T>> listeners = null;

    public HQuery(final HConnection connection, final String query) throws IOException, HPersistException {

        this.connection = connection;
        this.query = query;

        final QueryArgs args = (QueryArgs)HBqlRule.SELECT.parse(this.getQuery(), (ExprSchema)null);
        this.schema = (HBaseSchema)args.getSchema();

        final WhereArgs where = args.getWhereExpr();

        this.fieldList = (args.getColumns() == null) ? this.getSchema().getFieldList() : args.getColumns();

        this.clientFilter = this.getExprTree(where.getClientFilter(), this.getSchema(), fieldList);

        final ExprTree serverFilter = this.getExprTree(where.getServerFilter(),
                                                       HUtil.getServerSchema(this.getSchema()),
                                                       fieldList);

        this.scanList = HUtil.getScanList(this.getSchema(),
                                          fieldList,
                                          where.getKeyRangeArgs(),
                                          where.getDateRangeArgs(),
                                          where.getVersionArgs(),
                                          where.getLimitArgs(),
                                          serverFilter);
    }

    public synchronized void addListener(final HQueryListener<T> listener) {
        if (this.getListeners() == null)
            this.listeners = Lists.newArrayList();

        this.getListeners().add(listener);
    }

    List<Scan> getScanList() {
        return this.scanList;
    }

    HBaseSchema getSchema() {
        return this.schema;
    }

    ExprTree getClientFilter() {
        return this.clientFilter;
    }

    List<String> getFieldList() {
        return this.fieldList;
    }

    public String getQuery() {
        return this.query;
    }

    HConnection getConnection() {
        return this.connection;
    }

    public boolean isAnnotationSchema() {
        return this.schema instanceof AnnotationSchema;
    }

    public List<HQueryListener<T>> getListeners() {
        return this.listeners;
    }

    public void clearListeners() {
        if (this.getListeners() != null)
            this.getListeners().clear();
    }

    ExprTree getExprTree(final ExprTree exprTree,
                         final ExprSchema schema,
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

        if (this.getListeners() != null) {
            for (final HQueryListener<T> listener : this.getListeners())
                listener.onQueryInit();
        }

        return new HResults<T>(this);
    }
}
