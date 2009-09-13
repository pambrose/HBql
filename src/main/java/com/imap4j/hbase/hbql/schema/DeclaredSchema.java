package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.util.Maps;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;

import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 9, 2009
 * Time: 10:19:00 AM
 */
public class DeclaredSchema extends ExprSchema {

    private final static Map<String, DeclaredSchema> declaredSchemaMap = Maps.newHashMap();

    final String tableName;

    public DeclaredSchema(final TokenStream input, final List<VarDesc> varList) throws RecognitionException {
        this.tableName = "declared";
        try {
            for (final VarDesc var : varList) {
                final VarDescAttrib attrib = new VarDescAttrib(var);
                addVariableAttrib(attrib);
            }
        }
        catch (HPersistException e) {
            System.out.println(e.getMessage());
            throw new RecognitionException(input);
        }
    }

    private DeclaredSchema(final String tableName, final List<VarDesc> varList) throws HPersistException {
        this.tableName = tableName;
        for (final VarDesc var : varList) {
            final VarDescAttrib attrib = new VarDescAttrib(var);
            addVariableAttrib(attrib);
        }
    }

    public synchronized static DeclaredSchema newDeclaredSchema(final String tableName,
                                                                final List<VarDesc> varList) throws HPersistException {

        DeclaredSchema schema = getDeclaredSchemaMap().get(tableName);
        if (schema != null)
            throw new HPersistException("Table " + tableName + " already defined");

        schema = new DeclaredSchema(tableName, varList);
        getDeclaredSchemaMap().put(tableName, schema);
        return schema;
    }

    private static Map<String, DeclaredSchema> getDeclaredSchemaMap() {
        return declaredSchemaMap;
    }

    @Override
    public String getTableName() {
        return this.tableName;
    }

    @Override
    public String getSchemaName() {
        return this.getTableName();
    }

}
