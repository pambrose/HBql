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
                            + "family1:val1 string alias val1, "
                            + "family1:val2 string alias val2, "
                            + "family2:val1 string alias val3, "
                            + "family2:val2 string alias val4, "
                            + "family3:val1 string alias val5, "
                            + "family3:val2 string alias val6"
                            + ")");

        assertSelectColumnsMatchTrue("SELECT * FROM table1", "intValue");

    }

}