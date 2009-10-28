package org.apache.yaoql.client;

import org.apache.expreval.client.HBqlException;

public interface ObjectQueryListener<T> {

    void onQueryInit();

    void onEachObject(T val) throws HBqlException;

    void onQueryComplete();
}