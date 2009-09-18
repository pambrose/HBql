package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.hbql.query.antlr.cmds.SchemaManagerExecCmd;
import org.apache.hadoop.hbase.hbql.query.antlr.config.HBqlRule;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 18, 2009
 * Time: 2:14:47 PM
 */
public class SchemaManager {


    public static HOutput exec(final String str) throws HPersistException, IOException {

        final SchemaManagerExecCmd cmd = (SchemaManagerExecCmd)HBqlRule.SCHEMA_EXEC.parse(str);

        if (cmd == null)
            throw new HPersistException("Error parsing: " + str);

        return cmd.exec();
    }
}
