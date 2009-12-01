/*
 * Copyright (c) 2009.  The Apache Software Foundation
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

import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.util.TestSupport;
import org.apache.hadoop.hbase.jdbc.impl.ConnectionImpl;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class BasicTest extends TestSupport {

    static Connection connection = null;

    @BeforeClass
    public static void beforeClass() throws SQLException, ClassNotFoundException {

        Class.forName("org.apache.hadoop.hbase.jdbc.Driver");
        connection = DriverManager.getConnection("jdbc:hbql");

        Statement stmt = connection.createStatement();

        stmt.execute("CREATE TEMP MAPPING tab4 FOR TABLE table2"
                     + "("
                     + "keyval key, "
                     + "f1 ("
                     + "    val1 string alias val1, "
                     + "    val2 string alias val2, "
                     + "    val3 string alias notdefinedval "
                     + "), "
                     + "f2 ("
                     + "    val1 date alias val3, "
                     + "    val2 date alias val4 "
                     + "), "
                     + "f3 ("
                     + "    val1 int alias val5, "
                     + "    val2 int alias val6, "
                     + "    val3 int alias val7, "
                     + "    val4 int[] alias val8, "
                     + "    mapval1 object alias f3mapval1, "
                     + "    mapval2 object alias f3mapval2 "
                     + "))");

        HConnection conn = ((ConnectionImpl)connection).getHConnection();

        conn.execute("create table table2 (f1(), f2(), f3()) if not tableexists('table2')");
        conn.execute("delete from tab4");
    }


    @Test
    public void simpleQuery() throws SQLException {

        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from tab4");

        while (rs.next()) {
            int val5 = rs.getInt("val5");
            int val6 = rs.getInt("val6");
            String val1 = rs.getString("val1");
            String val2 = rs.getString("val2");

            System.out.print("val5: " + val5);
            System.out.print(", val6: " + val6);
            System.out.print(", val1: " + val1);
            System.out.println(", val2: " + val2);
        }
    }

    @Test
    public void simpleQueryWithNamedParams() throws SQLException {

        PreparedStatement stmt = connection.prepareStatement("select * from tab4 WITH CLIENT FILTER WHERE :val1 = :val2");
        stmt.setString(1, "aaa");
        stmt.setString(2, "aaa");
        ResultSet rs = stmt.executeQuery();

        while (rs.next()) {
            int val5 = rs.getInt("val5");
            int val6 = rs.getInt("val6");
            String val1 = rs.getString("val1");
            String val2 = rs.getString("val2");

            System.out.print("val5: " + val5);
            System.out.print(", val6: " + val6);
            System.out.print(", val1: " + val1);
            System.out.println(", val2: " + val2);
        }
    }

    @Test
    public void simpleQueryWithUnNamedParams() throws SQLException {

        PreparedStatement stmt = connection.prepareStatement("select * from tab4 WITH CLIENT FILTER WHERE ? = ?");
        stmt.setString(1, "aaa");
        stmt.setString(2, "aaa");
        ResultSet rs = stmt.executeQuery();

        while (rs.next()) {
            int val5 = rs.getInt("val5");
            int val6 = rs.getInt("val6");
            String val1 = rs.getString("val1");
            String val2 = rs.getString("val2");

            System.out.print("val5: " + val5);
            System.out.print(", val6: " + val6);
            System.out.print(", val1: " + val1);
            System.out.println(", val2: " + val2);
        }

        stmt.clearParameters();
        ResultSet rs2 = stmt.executeQuery();
    }

    @Test
    public void urlTest1() throws SQLException {

        SQLException exception = null;

        try {
            Connection conn = DriverManager.getConnection("jdbc:hbql;unknown=44");
        }
        catch (SQLException e) {
            e.printStackTrace();
            exception = e;
        }

        assertTrue(exception != null);
    }

    @Test
    public void urlTest2() throws SQLException {

        Exception exception = null;

        try {
            Connection conn = DriverManager.getConnection("jdbc:hbql;maxtablerefs=dd");
        }
        catch (Exception e) {
            e.printStackTrace();
            exception = e;
        }

        assertTrue(exception != null);
    }

    @Test
    public void urlTest3() throws SQLException {

        SQLException exception = null;
        int maxrefs = 0;

        try {
            Connection conn = DriverManager.getConnection("jdbc:hbql;maxtablerefs=44");
            ConnectionImpl connimpl = (ConnectionImpl)conn;
            maxrefs = connimpl.getHConnectionImpl().getMaxTablePoolReferencesPerTable();
        }
        catch (SQLException e) {
            e.printStackTrace();
            exception = e;
        }

        assertTrue(exception == null);
        assertTrue(maxrefs == 44);
    }
}
