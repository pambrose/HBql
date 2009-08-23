package com.imap4j.hbase.hbql;

import com.google.common.collect.Lists;
import com.imap4j.hbase.antlr.QueryArgs;
import com.imap4j.hbase.antlr.config.HBqlRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 9:26:38 PM
 */
public class Query<T extends Persistable> {

    final String query;
    final QueryListener<T> listener;

    public Query(final String query, final QueryListener<T> listener) {
        this.query = query;
        this.listener = listener;
    }

    public void execute() throws IOException, PersistException {

        final QueryArgs qa = (QueryArgs)HBqlRule.QUERY.parse(this.query);

        final ClassSchema classSchema = ClassSchema.getClassSchema(qa.getTableName());

        final HTable table = new HTable(new HBaseConfiguration(), classSchema.getTableName());

        final List<String> colList = Lists.newArrayList();

        try {
            final List<String> fieldList = (qa.getColumnList() == null)
                                           ? ClassSchema.getClassSchema(qa.getTableName()).getFieldList()
                                           : qa.getColumnList();

            for (final String attribName : fieldList) {
                final FieldAttrib attrib = classSchema.getFieldAttribMapByField().get(attribName);
                // If it is a map, then request all columns for family
                if (attrib.isMapKeysAsColumns())
                    colList.add(attrib.getFamilyName());
                else
                    colList.add(attrib.getQualifiedName());
            }

            final Scan scan = new Scan();

            for (final String col : colList)
                scan.addColumn(col.getBytes());

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
                        final String mapKey = column.substring(lbrace + 1, column.length() - 1);
                        final String mapcol = column.substring(0, lbrace);

                    }
                    else {
                        final FieldAttrib attrib = classSchema.getFieldAttribMapByColumn().get(column);
                        final Object val = attrib.getValueFromBytes(newobj, valbytes);
                        attrib.getField().set(newobj, val);
                    }
                }

                this.listener.onEachRow(newobj);
            }

        }
        catch (Exception e) {
            e.printStackTrace();
            throw new PersistException("Error in execute()");
        }

    }
}
