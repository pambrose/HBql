package org.apache.hadoop.hbase.hbql;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.util.TestSupport;
import org.junit.Test;

public class WithExpressionsTest extends TestSupport {

    @Test
    public void keysExpressions() throws HBqlException {
        assertValidInput("WITH KEYS 'aaa' TO 'bbb'");
        assertValidInput("WITH KEYS 'sss' TO LAST");
        assertValidInput("WITH KEYS 'fff' TO 'ggg', 'sss'TO LAST, 'sssd' TO LAST");
    }

    @Test
    public void timeExpressions() throws HBqlException {
        assertValidInput("WITH TIME RANGE NOW() TO NOW()");
        assertValidInput("WITH TIME RANGE NOW() TO NOW()+DAY(1)");
    }

    @Test
    public void versionExpressions() throws HBqlException {
        assertValidInput("WITH VERSIONS 12");
    }

    @Test
    public void timerangeExpressions() throws HBqlException {
        assertValidInput("WITH TIME RANGE NOW() TO NOW()+DAY(1)");
        assertValidInput("WITH TIME RANGE NOW() - DAY(1) TO NOW() + DAY(1) + DAY(2)");
        assertValidInput("WITH TIME RANGE DATE('10/31/94', 'mm/dd/yy') - DAY(1) TO NOW()+DAY(1) + DAY(2)");
    }

    @Test
    public void clientFilterExpressions() throws HBqlException {
        assertValidInput("WITH CLIENT FILTER WHERE TRUE");
        assertValidInput("WITH CLIENT FILTER WHERE {col1 int} col1 < 4");
        assertValidInput("WITH CLIENT FILTER WHERE {fam1:col1 int} fam1:col1 < 4");
    }

    @Test
    public void serverFilterExpressions() throws HBqlException {
        assertValidInput("WITH SERVER FILTER WHERE TRUE");
        assertValidInput("WITH SERVER FILTER WHERE {col1 int} col1 < 4");
        assertValidInput("WITH SERVER FILTER WHERE {fam1:col1 int alias d} fam1:col1 < 4 OR d > 3");
    }
}