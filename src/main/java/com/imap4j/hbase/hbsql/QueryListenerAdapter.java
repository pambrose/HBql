package com.imap4j.hbase.hbsql;

import java.io.Serializable;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 10:39:59 PM
 */
public abstract class QueryListenerAdapter<T extends Persistable> implements QueryListener<T>, Serializable {

    public void onQueryInit() {
    }

    public void onEachRow(T val) throws PersistException {
    }

    public void onQueryCompletion() {
    }
}
