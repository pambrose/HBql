package org.apache.hadoop.hbase.contrib.hbql.client;

import org.apache.expreval.client.HBqlException;

import java.util.List;

public interface HQuery<T> {

    HConnection getConnection();

    List<HQueryListener<T>> getListeners();

    void setParameter(String name, Object val) throws HBqlException;

    void addListener(HQueryListener<T> listener);

    void clearListeners();

    HResults<T> getResults();

    List<T> getResultList() throws HBqlException;
}
