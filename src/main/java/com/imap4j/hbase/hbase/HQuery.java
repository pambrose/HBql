package com.imap4j.hbase.hbase;

import com.imap4j.hbase.antlr.args.QueryArgs;
import com.imap4j.hbase.antlr.config.HBqlRule;
import com.imap4j.hbase.hbql.expr.ExprTree;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.schema.AnnotationSchema;
import com.imap4j.hbase.hbql.schema.ExprSchema;
import com.imap4j.hbase.hbql.schema.HUtil;
import com.imap4j.hbase.util.Lists;
import com.imap4j.hbase.util.ResultsIterator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 9:26:38 PM
 */
public class HQuery<T extends HPersistable> implements Iterable<T> {

    final String query;
    final HQueryListener<T> listener;
    final List<ResultScanner> scannerList = Lists.newArrayList();

    private HQuery(final String query, final HQueryListener<T> listener) {
        this.query = query;
        this.listener = listener;
    }

    public static <T extends HPersistable> HQuery<T> newHQuery(final String query, final HQueryListener<T> listener) {
        return new HQuery<T>(query, listener);
    }

    public static <T extends HPersistable> HQuery<T> newHQuery(final String query) {
        return new HQuery<T>(query, null);
    }

    public String getQuery() {
        return this.query;
    }

    public HQueryListener<T> getListener() {
        return this.listener;
    }

    private ExprTree getExprTree(final ExprTree exprTree,
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

    public void execute() throws IOException, HPersistException {

        if (this.getListener() == null)
            return;

        final QueryArgs args = (QueryArgs)HBqlRule.SELECT.parse(this.getQuery(), (ExprSchema)null);
        final AnnotationSchema schema = AnnotationSchema.getAnnotationSchema(args.getTableName());
        final List<String> fieldList = (args.getColumnList() == null) ? schema.getFieldList() : args.getColumnList();

        final ExprTree clientExprTree = getExprTree(args.getWhereExpr().getClientFilter(), schema, fieldList);

        final List<Scan> scanList = HUtil.getScanList(schema,
                                                      fieldList,
                                                      args.getWhereExpr().getKeyRange(),
                                                      args.getWhereExpr().getVersion(),
                                                      getExprTree(args.getWhereExpr().getServerFilter(), schema, fieldList));

        final HTable table = new HTable(new HBaseConfiguration(), schema.getTableName());

        for (final Scan scan : scanList) {
            ResultScanner resultScanner = null;
            try {
                resultScanner = table.getScanner(scan);
                for (final Result result : resultScanner) {
                    final HPersistable val = HUtil.getHPersistable(HUtil.ser,
                                                                   schema,
                                                                   fieldList,
                                                                   scan.getMaxVersions(),
                                                                   result);
                    if (clientExprTree == null || clientExprTree.evaluate(val))
                        this.getListener().onEachRow((T)val);
                }
            }
            finally {
                if (resultScanner != null)
                    resultScanner.close();
            }
        }
    }

    private List<ResultScanner> getScannerList() {
        return scannerList;
    }

    public void close() {

        for (final ResultScanner scanner : this.getScannerList())
            closeCurrentScanner(scanner, false);

        this.scannerList.clear();
    }

    private void closeCurrentScanner(final ResultScanner scanner, final boolean removeFromList) {
        if (scanner == null)
            return;

        try {
            scanner.close();
        }
        catch (Exception e) {
            // Do nothing
        }

        if (removeFromList)
            getScannerList().remove(scanner);
    }

    @Override
    public Iterator<T> iterator() {

        try {
            return new ResultsIterator<T>() {

                final QueryArgs args = (QueryArgs)HBqlRule.SELECT.parse(getQuery(), (ExprSchema)null);
                final AnnotationSchema schema = AnnotationSchema.getAnnotationSchema(args.getTableName());
                final List<String> fieldList = (args.getColumnList() == null) ? schema.getFieldList() : args.getColumnList();

                final ExprTree clientExprTree = getExprTree(args.getWhereExpr().getClientFilter(), schema, fieldList);

                final List<Scan> scanList = HUtil.getScanList(this.schema,
                                                              this.fieldList,
                                                              this.args.getWhereExpr().getKeyRange(),
                                                              this.args.getWhereExpr().getVersion(),
                                                              getExprTree(args.getWhereExpr().getServerFilter(),
                                                                          this.schema,
                                                                          this.fieldList));

                final HTable table = new HTable(new HBaseConfiguration(), schema.getTableName());
                final Iterator<Scan> scanIter = scanList.iterator();
                int maxVersions = 0;
                ResultScanner currentResultScanner = null;
                Iterator<Result> resultIter = null;

                // Prime the iterator with the first value
                T nextObject = fetchNextObject();

                private Iterator<Result> getNextResultScanner() throws IOException {
                    if (scanIter.hasNext()) {

                        final Scan scan = scanIter.next();
                        maxVersions = scan.getMaxVersions();

                        // First close previous ResultScanner before reassigning
                        closeCurrentScanner(currentResultScanner, true);

                        currentResultScanner = table.getScanner(scan);
                        getScannerList().add(currentResultScanner);

                        return currentResultScanner.iterator();
                    }
                    else {
                        return null;
                    }
                }

                protected T fetchNextObject() throws HPersistException, IOException {

                    T val = doFetch();

                    if (val != null)
                        return val;

                    // Try one more time
                    val = doFetch();
                    if (val == null)
                        closeCurrentScanner(currentResultScanner, true);
                    return val;
                }

                private T doFetch() throws HPersistException, IOException {

                    if (resultIter == null)
                        resultIter = getNextResultScanner();

                    if (resultIter != null) {
                        while (resultIter.hasNext()) {
                            final Result result = resultIter.next();
                            final T val = (T)HUtil.getHPersistable(HUtil.ser, schema, fieldList, maxVersions, result);
                            if (this.clientExprTree == null || this.clientExprTree.evaluate(val))
                                return val;

                        }
                    }

                    // Reset to get next scanner
                    resultIter = null;
                    return null;
                }

                protected T getNextObject() {
                    return this.nextObject;
                }

                protected void setNextObject(final T nextObject) {
                    this.nextObject = nextObject;
                }
            };
        }
        catch (HPersistException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public T next() {
                return null;
            }

            @Override
            public void remove() {

            }
        };
    }

}
