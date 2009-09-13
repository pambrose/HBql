package com.imap4j.hbase.hbase;

import com.imap4j.hbase.antlr.args.QueryArgs;
import com.imap4j.hbase.antlr.config.HBqlRule;
import com.imap4j.hbase.hbql.expr.ExprTree;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.schema.AnnotationSchema;
import com.imap4j.hbase.hbql.schema.DeclaredSchema;
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
    final ExprTree clientExprTree;
    final List<Scan> scanList;

    final boolean useAnnotations;

    List<HQueryListener<T>> listeners = null;

    public HQuery(final HConnection connection, final String query) throws IOException, HPersistException {
        this.connection = connection;
        this.query = query;

        final QueryArgs args = (QueryArgs)HBqlRule.SELECT.parse(this.getQuery(), (ExprSchema)null);
        this.schema = this.findSchema(args.getTableName());

        this.useAnnotations = this.schema instanceof AnnotationSchema;
        this.fieldList = (args.getColumns() == null) ? this.getSchema().getFieldList() : args.getColumns();

        this.clientExprTree = this.getExprTree(args.getWhereExpr().getClientFilter(),
                                               this.getSchema(),
                                               fieldList);

        this.scanList = HUtil.getScanList(this.getSchema(),
                                          fieldList,
                                          args.getWhereExpr().getKeyRange(),
                                          args.getWhereExpr().getVersion(),
                                          this.getExprTree(args.getWhereExpr().getServerFilter(),
                                                           this.getSchema(),
                                                           fieldList));
    }

    private HBaseSchema findSchema(final String tableName) throws HPersistException {

        // First look in AnnotationSchema and then try DeclaredSchemas
        HBaseSchema schema = AnnotationSchema.getAnnotationSchema(tableName);

        if (schema != null)
            return schema;

        schema = DeclaredSchema.getDeclaredSchema(tableName);

        if (schema != null)
            return schema;

        throw new HPersistException("Unknown table name " + tableName);

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

    ExprTree getClientExprTree() {
        return this.clientExprTree;
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

    public boolean useAnnotations() {
        return this.useAnnotations;
    }

    private List<HQueryListener<T>> getListeners() {
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

        final HResults<T> retval = new HResults<T>(this);

        if (this.getListeners() != null && this.getListeners().size() > 0) {
            try {
                for (final HQueryListener<T> listener : this.getListeners())
                    listener.onQueryInit();

                for (final T val : retval)
                    for (final HQueryListener<T> listener : this.getListeners())
                        listener.onEachRow(val);

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
