package org.apache.hadoop.hbase.hbql.client;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Oct 6, 2009
 * Time: 12:10:30 PM
 */
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
