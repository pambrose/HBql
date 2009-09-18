package org.apache.hadoop.hbase.hbql.query.antlr.cmds;

import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HOutput;
import org.apache.hadoop.hbase.hbql.client.HPersistException;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 23, 2009
 * Time: 10:46:03 PM
 */
public interface ExecCmd {

    public HOutput exec(final HConnection conn) throws HPersistException, IOException;

}
