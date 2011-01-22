/*
 * Copyright (c) 2011.  The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.jdbc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.util.Maps;
import org.apache.hadoop.hbase.jdbc.impl.ConnectionImpl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class Driver implements java.sql.Driver {

    static {
        try {
            DriverManager.registerDriver(new Driver());
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static Map<String, String> getArgMap(final String str) throws SQLException {
        final Map<String, String> retval = Maps.newHashMap();

        for (final String arg : str.split(";")) {
            if (arg.contains("=")) {
                final String[] vals = arg.split("=");
                final String var = vals[0].toLowerCase();

                if (!var.equals(HConnectionImpl.MAXTABLEREFS)
                    && !var.equals(HConnectionImpl.MASTER))
                    throw new SQLException("Unknown JDBC URL option: " + var);

                retval.put(var, vals[1]);
            }
        }
        return retval;
    }

    public Connection connect(final String url, final Properties properties) throws SQLException {

        final Map<String, String> argMap = getArgMap(url);

        final Configuration config;
        if (argMap.containsKey(HConnectionImpl.MASTER))
            config = HConnectionImpl.getConfiguration(argMap.get(HConnectionImpl.MASTER));
        else
            config = null;

        return getConnection(url, config);
    }

    public static Connection getConnection(final String url, final Configuration config) throws SQLException {
        if (!validURL(url))
            return null;

        final Map<String, String> argMap = getArgMap(url);

        final int maxtablerefs = argMap.containsKey(HConnectionImpl.MAXTABLEREFS)
                                 ? Integer.parseInt(argMap.get(HConnectionImpl.MAXTABLEREFS)) : Integer.MAX_VALUE;
        return new ConnectionImpl(config, maxtablerefs);
    }

    public boolean acceptsURL(final String url) throws SQLException {
        return validURL(url);
    }

    private static boolean validURL(final String url) throws SQLException {
        return (url != null && url.toLowerCase().startsWith("jdbc:hbql"));
    }

    public DriverPropertyInfo[] getPropertyInfo(final String s, final Properties properties) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    public int getMajorVersion() {
        return 1;
    }

    public int getMinorVersion() {
        return 0;
    }

    public boolean jdbcCompliant() {
        return false;
    }
}
