package org.apache.hadoop.hbase.contrib.hbql.client;

import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.contrib.hbql.impl.RecordImpl;
import org.apache.hadoop.hbase.contrib.hbql.parser.HBqlShell;
import org.apache.hadoop.hbase.contrib.hbql.schema.AnnotationSchema;
import org.apache.hadoop.hbase.contrib.hbql.schema.ColumnDescription;
import org.apache.hadoop.hbase.contrib.hbql.schema.DefinedSchema;
import org.apache.hadoop.hbase.contrib.hbql.schema.HBaseSchema;
import org.apache.hadoop.hbase.contrib.hbql.statement.SchemaManagerStatement;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class SchemaManager {

    private final static Map<String, DefinedSchema> definedSchemaMap = Maps.newHashMap();

    public static Output execute(final String str) throws HBqlException {
        final SchemaManagerStatement cmd = HBqlShell.parseSchemaManagerStatement(str);
        return cmd.execute();
    }

    private static Map<String, DefinedSchema> getDefinedSchemaMap() {
        return SchemaManager.definedSchemaMap;
    }

    public static Set<String> getDefinedSchemaNames() {
        return getDefinedSchemaMap().keySet();
    }

    public static DefinedSchema getDefinedSchema(final String schemaName) {
        return SchemaManager.getDefinedSchemaMap().get(schemaName);
    }

    public static AnnotationSchema getAnnotationSchema(final String schemaName) throws HBqlException {
        return AnnotationSchema.getAnnotationSchema(schemaName);
    }

    public static boolean doesDefinedSchemaExist(final String schemaName) {
        return null != SchemaManager.getDefinedSchemaMap().get(schemaName);
    }

    public static void dropSchema(final String schemaName) {
        if (SchemaManager.getDefinedSchemaMap().containsKey(schemaName))
            SchemaManager.getDefinedSchemaMap().remove(schemaName);
    }

    public synchronized static DefinedSchema newDefinedSchema(final String schemaName,
                                                              final String tableName,
                                                              final List<ColumnDescription> colList) throws HBqlException {

        if (SchemaManager.doesDefinedSchemaExist(schemaName))
            throw new HBqlException("Schema " + schemaName + " already defined");

        final DefinedSchema schema = new DefinedSchema(schemaName, tableName, colList);

        SchemaManager.getDefinedSchemaMap().put(schemaName, schema);

        return schema;
    }

    public static Record newRecord(final String schemaName) throws HBqlException {
        final HBaseSchema schema = getSchema(schemaName);
        return new RecordImpl(schema);
    }

    public static HBaseSchema getSchema(final String schemaName) throws HBqlException {

        // First look in defined schema, then try annotation schema
        HBaseSchema schema;

        schema = getDefinedSchema(schemaName);
        if (schema != null)
            return schema;

        schema = getAnnotationSchema(schemaName);
        if (schema != null)
            return schema;

        throw new HBqlException("Unknown schema: " + schemaName);
    }
}
