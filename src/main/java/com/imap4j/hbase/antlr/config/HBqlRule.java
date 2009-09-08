package com.imap4j.hbase.antlr.config;

import com.imap4j.imap.antlr.util.GrammarRule;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 20, 2009
 * Time: 9:56:22 PM
 */
public enum HBqlRule {

    SELECT(HBqlGrammar.HBql, "selectStmt"),
    EXEC(HBqlGrammar.HBql, "execCommand"),
    WHERE_EXPR(HBqlGrammar.HBql, "whereExpr"),
    WHERE_VALUE(HBqlGrammar.HBql, "whereValue");

    private final GrammarRule grammarRule;

    HBqlRule(final HBqlGrammar grammar, final String rule) {
        this.grammarRule = (grammar != null) ? GrammarRule.newInstance(grammar.getGrammarDef(), rule) : null;
    }

    private GrammarRule getGrammarRule() {
        return grammarRule;
    }

    public Object parse(final String str, Object... args) {
        return this.getGrammarRule().parse(str, args);
    }

}
