package org.apache.hadoop.hbase.hbql.query.antlr.config;

import com.imap4j.imap.antlr.util.GrammarRule;
import org.apache.hadoop.hbase.hbql.client.HPersistException;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 9:56:22 PM
 */
public enum HBqlRule {

    SELECT(HBqlGrammar.HBql, "selectStmt"),
    CONNECTION_EXEC(HBqlGrammar.HBql, "connectionExec"),
    SCHEMA_PARSE(HBqlGrammar.HBql, "schemaExec"),
    NODESC_WHERE_EXPR(HBqlGrammar.HBql, "nodescWhereExpr"),
    DESC_WHERE_VALUE(HBqlGrammar.HBql, "descWhereExpr"),
    WITH_EXPR(HBqlGrammar.HBql, "whereValue"),
    STRING_EXPR(HBqlGrammar.HBql, "stringExpr"),
    NUMBER_EXPR(HBqlGrammar.HBql, "numericTest"),
    DATE_EXPR(HBqlGrammar.HBql, "dateTest");

    private final GrammarRule grammarRule;

    HBqlRule(final HBqlGrammar grammar, final String rule) {
        this.grammarRule = (grammar != null) ? GrammarRule.newInstance(grammar.getGrammarDef(), rule) : null;
    }

    private GrammarRule getGrammarRule() {
        return grammarRule;
    }

    public Object parse(final String str, Object... args) throws HPersistException {
        final Object retval = this.getGrammarRule().parse(str, args);
        if (retval == null)
            throw new HPersistException("Error parsing " + str);
        else
            return retval;
    }

}
