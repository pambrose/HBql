package org.apache.hadoop.hbase.hbql;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.SchemaManager;
import org.apache.hadoop.hbase.hbql.util.TestSupport;
import org.junit.Test;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 27, 2009
 * Time: 2:13:47 PM
 */
public class SelectExpressionsTest extends TestSupport {


    @Test
    public void selectExpressions() throws HBqlException {

        SchemaManager.parse("define table table1 alias tab1"
                            + "("
                            + "keyval key, "
                            + "f1:val1 string alias val1, "
                            + "f1:val2 string alias val2, "
                            + "f2:val1 string alias val3, "
                            + "f2:val2 string alias val4, "
                            + "f3:val1 string alias val5, "
                            + "f3:val2 string alias val6"
                            + ")");

        assertSelectElementsMatchTrue("SELECT f1:val1 FROM table1", "f1:val1");

        assertSelectElementsMatchTrue("SELECT * FROM table1", "f1:val1, f1:val2, f2:val1, f2:val2, f3:val1, f3:val2");
        assertSelectElementsMatchTrue("SELECT * FROM tab1", "f1:val1, f1:val2, f2:val1, f2:val2, f3:val1, f3:val2");

        assertSelectElementsMatchTrue("SELECT f1:* FROM table1", "f1:val1, f1:val2");
        assertSelectElementsMatchTrue("SELECT f1:*, f3:* FROM table1", "f1:val1, f1:val2, f3:val1, f3:val2");
        assertSelectElementsMatchTrue("SELECT f1:*, f2:*, f3:* FROM tab1", "f1:val1, f1:val2, f2:val1, f2:val2, f3:val1, f3:val2");
        assertSelectElementsMatchTrue("SELECT f1:*, f2:val1 FROM tab1", "f1:val1, f1:val2, f2:val1");
        assertSelectElementsMatchTrue("SELECT f1:*, f2:val1, f3:* FROM tab1", "f1:val1, f1:val2, f2:val1, f3:val1, f3:val2");

        assertSelectElementsMatchTrue("SELECT f1:val1, f2:val1, f3:* FROM tab1", "f1:val1, f2:val1, f3:val1, f3:val2");

    }

}