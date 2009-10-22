package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.hbql.query.antlr.HBql;
import org.apache.hadoop.hbase.hbql.query.cmds.SchemaManagerCmd;
import org.apache.hadoop.hbase.hbql.query.impl.hbase.HRecordImpl;
import org.apache.hadoop.hbase.hbql.query.schema.AnnotationSchema;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnDescription;
import org.apache.hadoop.hbase.hbql.query.schema.DefinedSchema;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.query.schema.ReflectionSchema;
import org.apache.hadoop.hbase.hbql.query.schema.Schema;
import org.apache.hadoop.hbase.hbql.query.util.Maps;

import java.util.List;
import java.util.Map;

public class SchemaManager {

    private final static Map<String, DefinedSchema> definedSchemaMap = Maps.newHashMap();

    public static HOutput execute(final String str) throws HBqlException {

        final SchemaManagerCmd cmd = HBql.parseSchemaCommand(str);

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

    private static Map<String, DefinedSchema> getDefinedSchemaMap() {
        return definedSchemaMap;
    }

    public static DefinedSchema getDefinedSchema(final String tableName) {
        return getDefinedSchemaMap().get(tableName);
    }

    public static boolean doesDefinedSchemaExist(final String tableName) {
        return null != getDefinedSchemaMap().get(tableName);
    }

    public static void dropSchema(final String name) {

        final DefinedSchema schema = getDefinedSchema(name);
        if (schema != null) {

            if (getDefinedSchemaMap().containsKey(schema.getTableName()))
                getDefinedSchemaMap().remove(schema.getTableName());

            if (getDefinedSchemaMap().containsKey(schema.getTableAliasName()))
                getDefinedSchemaMap().remove(schema.getTableAliasName());
        }
    }

    public synchronized static DefinedSchema newDefinedSchema(final String tableName,
                                                              final String aliasName,
                                                              final List<ColumnDescription> colList) throws HBqlException {

        if (SchemaManager.doesDefinedSchemaExist(tableName))
            throw new HBqlException("Schema " + tableName + " already defined");

        if (aliasName != null && SchemaManager.doesDefinedSchemaExist(aliasName))
            throw new HBqlException("Alias " + aliasName + " already defined");

        final DefinedSchema schema = new DefinedSchema(tableName, aliasName, colList);

        getDefinedSchemaMap().put(tableName, schema);

        // Add in the same schema if there is an alias
        if (aliasName != null && !tableName.equals(aliasName))
            getDefinedSchemaMap().put(aliasName, schema);

        return schema;
    }

    public static HRecord newHRecord(final String tableName) throws HBqlException {
        final HBaseSchema schema = HBaseSchema.findSchema(tableName);
        return new HRecordImpl(schema);
    }
}
