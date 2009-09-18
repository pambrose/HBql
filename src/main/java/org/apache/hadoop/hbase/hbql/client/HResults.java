package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
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
        return hquery;
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

                final HTable table = getHQuery().getConnection().getHTable(getHQuery().getSchema().getTableName());
                final ExprTree clientExprTree = getHQuery().getClientExprTree();
                final Iterator<Scan> scanIter = getHQuery().getScanList().iterator();
                int maxVersions = 0;
                ResultScanner currentResultScanner = null;
                Iterator<Result> resultIter = null;
                long recordCount = 0;

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
                            final T val = (T)getHQuery().getSchema().getObject(getHQuery().getFieldList(),
                                                                               this.maxVersions,
                                                                               result);

                            if (clientExprTree == null || clientExprTree.evaluate(val)) {
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

                protected T getNextObject() {
                    return this.nextObject;
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
