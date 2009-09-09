package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbql.HPersistException;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 9, 2009
 * Time: 10:19:00 AM
 */
public class DeclaredSchema extends ExprSchema {

    public DeclaredSchema(final TokenStream input, final List<VarDesc> varList) throws RecognitionException {

        try {
            for (final VarDesc var : varList) {
                final VarDescAttrib attrib = new VarDescAttrib(var.getVarName(), var.getType());
                setVariableAttribByVariableName(var.getVarName(), attrib);
            }
        }
        catch (HPersistException e) {
            System.out.println(e.getMessage());
            throw new RecognitionException(input);
        }
    }

}
