package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.schema.VariableAttrib;
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

    final List<ResultScanner> scannerList = Lists.newArrayList();
    final HQuery hquery;

    public HResults(final HQuery hquery) {
        this.hquery = hquery;
    }

    private HQuery getHQuery() {
        return this.hquery;
    }

    private List<ResultScanner> getScannerList() {
        return this.scannerList;
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

                final HTable table = getHQuery().getConnection().getHTable(getHQuery().getSchema().getTableName());
                final ExprTree clientExprTree = getHQuery().getClientExprTree();
                final Iterator<Scan> scanIter = getHQuery().getScanList().iterator();
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

                private Iterator<Result> getNextResultScanner() throws IOException {
                    if (this.getScanIter().hasNext()) {

                        final Scan scan = this.getScanIter().next();
                        maxVersions = scan.getMaxVersions();

                        // First close previous ResultScanner before reassigning
                        closeCurrentScanner(this.getCurrentResultScanner(), true);

                        currentResultScanner = table.getScanner(scan);
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

                            final List<VariableAttrib> attribList = getHQuery().getSelectAttribList();

                            final T val = (T)getHQuery().getSchema().newObject(attribList, this.maxVersions, result);

                            if (getClientExprTree() == null || getClientExprTree().evaluate(val)) {
                                this.recordCount++;
                                final List<HQueryListener<T>> listenerList = getHQuery().getListeners();
                                if (listenerList != null)
                                    for (final HQueryListener<T> listener : listenerList)
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

                    if (nextObject == null && !fromExceptionCatch) {
                        final List<HQueryListener<T>> listenerList = getHQuery().getListeners();
                        if (listenerList != null)
                            for (final HQueryListener<T> listener : listenerList)
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
