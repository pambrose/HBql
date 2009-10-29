package org.apache.hadoop.hbase.contrib.hbql.impl;

import java.io.IOException;

public interface BatchAction {

    void apply(final org.apache.hadoop.hbase.client.HTable table) throws IOException;
}
