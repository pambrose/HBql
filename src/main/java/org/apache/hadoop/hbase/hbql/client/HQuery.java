package org.apache.hadoop.hbase.hbql.client;

import java.util.List;

public interface HQuery<T> {

    HConnection getConnection();

    String getQuery();

    List<HQueryListener<T>> getListeners();

    void setParameter(String name, Object val) throws HBqlException;

    void addListener(HQueryListener<T> listener);

    void clearListeners();

    HResults<T> getResults() throws HBqlException;

    List<T> getResultList() throws HBqlException;
}
