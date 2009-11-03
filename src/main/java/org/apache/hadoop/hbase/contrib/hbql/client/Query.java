package org.apache.hadoop.hbase.contrib.hbql.client;

import java.util.List;

public interface Query<T> {

    Connection getConnection();

    List<QueryListener<T>> getListeners();

    void setParameter(String name, Object val) throws HBqlException;

    void addListener(QueryListener<T> listener);

    void clearListeners();

    Results<T> getResults();

    List<T> getResultList() throws HBqlException;
}
