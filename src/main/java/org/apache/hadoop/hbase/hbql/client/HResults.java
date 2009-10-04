package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.query.stmt.args.QueryArgs;
import org.apache.hadoop.hbase.hbql.query.stmt.args.WhereArgs;
import org.apache.hadoop.hbase.hbql.query.util.Lists;
import org.apache.hadoop.hbase.hbql.query.util.ResultsIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 12, 2009
 * Time: 2:08:38 PM
 */
public class HResults<T> implements Iterable<T> {

    private final List<ResultScanner> scannerList = Lists.newArrayList();
    private final HConnection connection;
    private final HQuery hquery;
    private final QueryArgs queryArgs;
    private final List<HQueryListener<T>> listeners;
    private final List<Scan> scanList;

    public HResults(final HQuery hquery,
                    final HConnection connection,
                    final QueryArgs queryArgs,
                    final List<HQueryListener<T>> listeners,
                    final List<Scan> scanList) {
        this.connection = connection;
        this.hquery = hquery;
        this.queryArgs = queryArgs;
        this.listeners = listeners;
        this.scanList = scanList;
    }

    private HConnection getConnection() {
        return connection;
    }

    private HQuery getHQuery() {
        return this.hquery;
    }

    private QueryArgs getQueryArgs() {
        return this.queryArgs;
    }

    private WhereArgs getWhereArgs() {
        return this.getQueryArgs().getWhereArgs();
    }

    private List<HQueryListener<T>> getListeners() {
        return listeners;
    }

    private List<ResultScanner> getScannerList() {
        return this.scannerList;
    }

    private List<Scan> getScanList() {
        return scanList;
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

                final HTable table = getConnection().getHTable(getQueryArgs().getSchema().getTableName());
                final ExprTree clientExprTree = getWhereArgs().getClientExprTree();
                final Iterator<Scan> scanIter = getScanList().iterator();

                int maxVersions = 0;
                ResultScanner currentResultScanner = null;
                Iterator<Result> resultIter = null;
                long recordCount = 0;

                // Prime the iterator with the first value
                T nextObject = fetchNextObject();

                private ExprTree getClientExprTree() {
                    return this.clientExprTree;
                }

                private Iterator<Scan> getScanIter() {
                    return this.scanIter;
                }

                private ResultScanner getCurrentResultScanner() {
                    return this.currentResultScanner;
                }

                private Iterator<Result> getResultIter() {
                    return this.resultIter;
                }

                protected T getNextObject() {
                    return this.nextObject;
                }

                private HTable getTable() {
                    return this.table;
                }

                private Iterator<Result> getNextResultScanner() throws IOException {
                    if (this.getScanIter().hasNext()) {

                        final Scan scan = this.getScanIter().next();
                        this.maxVersions = scan.getMaxVersions();

                        // First close previous ResultScanner before reassigning
                        closeCurrentScanner(this.getCurrentResultScanner(), true);

                        currentResultScanner = this.getTable().getScanner(scan);
                        getScannerList().add(this.getCurrentResultScanner());

                        return this.getCurrentResultScanner().iterator();
                    }
                    else {
                        return null;
                    }
                }

                protected T fetchNextObject() throws HBqlException, IOException {

                    T val = doFetch();

                    if (val != null)
                        return val;

                    // Try one more time
                    val = doFetch();
                    if (val == null)
                        closeCurrentScanner(this.getCurrentResultScanner(), true);
                    return val;
                }

                private T doFetch() throws HBqlException, IOException {

                    if (this.getResultIter() == null)
                        resultIter = getNextResultScanner();

                    if (this.getResultIter() != null) {

                        while (this.getResultIter().hasNext()) {

                            final Result result = this.getResultIter().next();

                            if (getClientExprTree() == null || getClientExprTree().evaluate(result)) {

                                this.recordCount++;

                                final HBaseSchema schema = getQueryArgs().getSchema();
                                final T val = (T)schema.newObject(getQueryArgs().getSelectAttribList(),
                                                                  getQueryArgs().getSelectElementList(),
                                                                  this.maxVersions,
                                                                  result);

                                if (getListeners() != null)
                                    for (final HQueryListener<T> listener : getListeners())
                                        listener.onEachRow(val);

                                return val;
                            }
                        }
                    }

                    // Reset to get next scanner
                    resultIter = null;
                    return null;
                }

                protected void setNextObject(final T nextObject, final boolean fromExceptionCatch) {

                    this.nextObject = nextObject;

                    if (nextObject == null && !fromExceptionCatch && getListeners() != null) {
                        for (final HQueryListener<T> listener : getListeners())
                            listener.onQueryComplete();

                    }
                }
            };
        }
        catch (HBqlException e) {
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
