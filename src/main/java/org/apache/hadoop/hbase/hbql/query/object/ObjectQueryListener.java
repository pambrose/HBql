package org.apache.hadoop.hbase.hbql.query.object;

import org.apache.hadoop.hbase.hbql.client.HPersistException;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 10:38:45 PM
 */
public interface ObjectQueryListener<T> {

    void onQueryInit();

    void onEachObject(T val) throws HPersistException;

    void onQueryComplete();

}