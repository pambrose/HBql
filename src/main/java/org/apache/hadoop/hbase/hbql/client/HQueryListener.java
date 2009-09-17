package org.apache.hadoop.hbase.hbql.client;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 10:38:45 PM
 */
public interface HQueryListener<T> {

    void onQueryInit();

    void onEachRow(T val) throws HPersistException;

    void onQueryComplete();

}
