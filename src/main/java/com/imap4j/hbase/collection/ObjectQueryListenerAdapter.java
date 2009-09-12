package com.imap4j.hbase.collection;

import com.imap4j.hbase.hbase.HPersistException;

import java.io.Serializable;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 10:39:59 PM
 */
public abstract class ObjectQueryListenerAdapter<T> implements ObjectQueryListener<T>, Serializable {

    public void onQueryInit() {
    }

    public void onEachObject(T val) throws HPersistException {
    }

    public void onQueryCompletion() {
    }
}