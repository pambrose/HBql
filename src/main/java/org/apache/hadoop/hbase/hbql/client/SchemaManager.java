package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.hbql.query.antlr.HBql;
import org.apache.hadoop.hbase.hbql.query.antlr.cmds.SchemaManagerCmd;
import org.apache.hadoop.hbase.hbql.query.schema.AnnotationSchema;
import org.apache.hadoop.hbase.hbql.query.schema.DefinedSchema;
import org.apache.hadoop.hbase.hbql.query.schema.ReflectionSchema;
import org.apache.hadoop.hbase.hbql.query.schema.Schema;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 18, 2009
 * Time: 2:14:47 PM
 */
public class SchemaManager {

    public static HOutput parse(final String str) throws HBqlException {

        final SchemaManagerCmd cmd = HBql.parseSchema(str);

        if (cmd == null)
            throw new HBqlException("Error parsing: " + str);

        return cmd.execute();
    }

    // This is used for tests
    public static Schema getObjectSchema(final Object recordObj) throws HBqlException {

        if (recordObj == null)
            return null;

        try {
            return AnnotationSchema.getAnnotationSchema(recordObj);
        }
        catch (HBqlException e) {
            // Not annotated properly
        }

        return ReflectionSchema.getReflectionSchema(recordObj);
    }

    public static DefinedSchema getDefinedSchema(final String tablename) throws HBqlException {
        return DefinedSchema.getDefinedSchema(tablename);
    }
}
