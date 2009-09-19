package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.hbql.query.antlr.HBql;
import org.apache.hadoop.hbase.hbql.query.antlr.cmds.SchemaManagerCmd;
import org.apache.hadoop.hbase.hbql.query.schema.AnnotationSchema;
import org.apache.hadoop.hbase.hbql.query.schema.DefinedSchema;
import org.apache.hadoop.hbase.hbql.query.schema.ReflectionSchema;
import org.apache.hadoop.hbase.hbql.query.schema.Schema;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 18, 2009
 * Time: 2:14:47 PM
 */
public class SchemaManager {

    public static HOutput parse(final String str) throws HPersistException, IOException {

        final SchemaManagerCmd cmd = HBql.parseSchema(str);

        if (cmd == null)
            throw new HPersistException("Error parsing: " + str);

        return cmd.exec();
    }

    // This is used for tests
    public static Schema getObjectSchema(final Object recordObj) throws HPersistException {

        if (recordObj == null)
            return null;

        try {
            return AnnotationSchema.getAnnotationSchema(recordObj);
        }
        catch (HPersistException e) {
            // Not annotated properly
        }

        return ReflectionSchema.getReflectionSchema(recordObj);
    }

    public static DefinedSchema getDefinedSchema(final String tablename) throws HPersistException {
        return DefinedSchema.getDefinedSchema(tablename);
    }
}
