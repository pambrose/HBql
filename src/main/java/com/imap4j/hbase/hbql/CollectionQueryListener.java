package com.imap4j.hbase.hbql;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 10:38:45 PM
 */
public interface CollectionQueryListener<T> {

    void onQueryInit();

    void onEachObject(T val) throws HPersistException;

    void onQueryCompletion();

}