package org.apache.hadoop.hbase.contrib.hbql.client;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.contrib.hbql.impl.HRecordImpl;
import org.apache.hadoop.hbase.contrib.hbql.parser.Parser;
import org.apache.hadoop.hbase.contrib.hbql.schema.AnnotationSchema;
import org.apache.hadoop.hbase.contrib.hbql.schema.ColumnDescription;
import org.apache.hadoop.hbase.contrib.hbql.schema.DefinedSchema;
import org.apache.hadoop.hbase.contrib.hbql.schema.HBaseSchema;
import org.apache.hadoop.hbase.contrib.hbql.schema.ReflectionSchema;
import org.apache.hadoop.hbase.contrib.hbql.schema.Schema;
import org.apache.hadoop.hbase.contrib.hbql.statement.SchemaManagerStatement;

import java.util.List;
import java.util.Map;

public class SchemaManager {

    private final static Map<String, DefinedSchema> definedSchemaMap = Maps.newHashMap();

    public static HOutput execute(final String str) throws HBqlException {
        final SchemaManagerStatement cmd = Parser.parseSchemaManagerStatement(str);
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

    private static Map<String, DefinedSchema> getDefinedSchemaMap() {
        return definedSchemaMap;
    }

    public static DefinedSchema getDefinedSchema(final String tableName) {
        return getDefinedSchemaMap().get(tableName);
    }

    public static boolean doesDefinedSchemaExist(final String tableName) {
        return null != getDefinedSchemaMap().get(tableName);
    }

    public static void dropSchema(final String schemaName) {
        if (getDefinedSchemaMap().containsKey(schemaName))
            getDefinedSchemaMap().remove(schemaName);
    }

    public synchronized static DefinedSchema newDefinedSchema(final String schemaName,
                                                              final String tableName,
                                                              final List<ColumnDescription> colList) throws HBqlException {

        if (SchemaManager.doesDefinedSchemaExist(schemaName))
            throw new HBqlException("Schema " + schemaName + " already defined");

        final DefinedSchema schema = new DefinedSchema(schemaName, tableName, colList);

        getDefinedSchemaMap().put(schemaName, schema);

        return schema;
    }

    public static HRecord newHRecord(final String schemaName) throws HBqlException {
        final HBaseSchema schema = HBaseSchema.findSchema(schemaName);
        return new HRecordImpl(schema);
    }
}
