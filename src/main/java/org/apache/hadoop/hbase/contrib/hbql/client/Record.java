package org.apache.hadoop.hbase.contrib.hbql.client;

import org.apache.expreval.client.HBqlException;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

public interface Record {

    void reset();

    void setTimestamp(final long timestamp);

    void setCurrentValue(String name, Object val) throws HBqlException;

    Object getCurrentValue(String name) throws HBqlException;

    Set<String> getColumnNameList() throws HBqlException;

    Map<Long, Object> getVersionMap(final String name);

    Map<String, Object> getKeysAsColumnsMap(String name);

    Map<String, NavigableMap<Long, Object>> getKeysAsColumnsVersionMap(String name);

    Map<String, byte[]> getFamilyDefaultValueMap(String name) throws HBqlException;

    Map<String, NavigableMap<Long, byte[]>> getFamilyDefaultVersionMap(String name) throws HBqlException;

    Map<String, Map<String, byte[]>> getFamilyDefaultKeysAsColumnsMap(String name) throws HBqlException;

    Map<String, Map<String, NavigableMap<Long, byte[]>>> getFamilyDefaultKeysAsColumnsVersionMap(String name) throws HBqlException;
}
