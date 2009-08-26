package com.imap4j.hbase.hql;

import com.google.common.collect.Maps;
import com.imap4j.hbase.antlr.QueryArgs;
import com.imap4j.hbase.antlr.config.HqlRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 9:26:38 PM
 */
public class HQuery<T extends HPersistable> {

    final String query;
    final HQueryListener<T> queryListener;

    public HQuery(final String query, final HQueryListener<T> queryListener) {
        this.query = query;
        this.queryListener = queryListener;
    }

    public String getQuery() {
        return this.query;
    }

    public HQueryListener<T> getQueryListener() {
        return this.queryListener;
    }

    public void execute() throws IOException, HPersistException {

        final QueryArgs qa = (QueryArgs)HqlRule.SELECT.parse(this.getQuery());

        final ClassSchema classSchema = ClassSchema.getClassSchema(qa.getTableName());

        final HTable table = new HTable(new HBaseConfiguration(), classSchema.getTableName());

        try {
            final List<String> fieldList = (qa.getColumnList() == null)
                                           ? ClassSchema.getClassSchema(qa.getTableName()).getFieldList()
                                           : qa.getColumnList();

            final Scan scan = new Scan();

            for (final String attribName : fieldList) {

                final FieldAttrib attrib = classSchema.getFieldAttribByField(attribName);

                // If it is a map, then request all columns for family
                if (attrib.isMapKeysAsColumns())
                    scan.addFamily(attrib.getFamilyName().getBytes());
                else
                    scan.addColumn(attrib.getQualifiedName().getBytes());
            }

            for (Result result : table.getScanner(scan)) {

                final T newobj = (T)classSchema.getClazz().newInstance();
                final FieldAttrib keyattrib = classSchema.getKeyFieldAttrib();
                final byte[] keybytes = result.getRow();
                final Object keyval = keyattrib.getValueFromBytes(newobj, keybytes);
                classSchema.getKeyFieldAttrib().getField().set(newobj, keyval);

                for (final KeyValue keyValue : result.list()) {

                    final byte[] colbytes = keyValue.getColumn();
                    final String column = new String(colbytes);

                    final byte[] valbytes = result.getValue(colbytes);

                    if (column.endsWith("]")) {

                        final int lbrace = column.indexOf("[");
                        final String mapcolumn = column.substring(0, lbrace);
                        final String mapKey = column.substring(lbrace + 1, column.length() - 1);
                        final FieldAttrib attrib = classSchema.getFieldAttribMapByColumn().get(mapcolumn);
                        final Object val = attrib.getValueFromBytes(newobj, valbytes);
                        Map mapval = (Map)attrib.getValue(newobj);

                        // TODO Not sure if it is kosher to create a map here
                        if (mapval == null) {
                            mapval = Maps.newHashMap();
                            attrib.getField().set(newobj, mapval);
                        }

                        mapval.put(mapKey, val);
                    }
                    else {
                        final FieldAttrib attrib = classSchema.getFieldAttribMapByColumn().get(column);
                        final Object val = attrib.getValueFromBytes(newobj, valbytes);
                        attrib.getField().set(newobj, val);
                    }
                }

                if (qa.getWhereExpr().evaluate(classSchema, newobj))
                    this.getQueryListener().onEachRow(newobj);
            }

        }
        catch (Exception e) {
            e.printStackTrace();
            throw new HPersistException("Error in execute()");
        }

    }
}
