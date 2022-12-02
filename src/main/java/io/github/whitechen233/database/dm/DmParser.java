package io.github.whitechen233.database.dm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.parser.Parser;
import org.flywaydb.core.internal.parser.ParserContext;
import org.flywaydb.core.internal.parser.ParsingContext;
import org.flywaydb.core.internal.parser.PeekingReader;
import org.flywaydb.core.internal.parser.Recorder;
import org.flywaydb.core.internal.parser.StatementType;
import org.flywaydb.core.internal.parser.Token;
import org.flywaydb.core.internal.parser.TokenType;
import org.flywaydb.core.internal.sqlscript.Delimiter;
import org.flywaydb.core.internal.sqlscript.ParsedSqlStatement;

public class DmParser extends Parser {

    private static final Delimiter PLSQL_DELIMITER = new Delimiter("/", true);
    private static final Pattern PLSQL_TYPE_BODY_REGEX = Pattern.compile(
        "^CREATE(\\sOR\\sREPLACE)?(\\s(NON)?EDITIONABLE)?\\sTYPE\\sBODY\\s(\\S*\\s)?(IS|AS)");
    private static final Pattern PLSQL_PACKAGE_BODY_REGEX = Pattern.compile(
        "^CREATE(\\s*OR\\s*REPLACE)?(\\s*(NON)?EDITIONABLE)?\\s*PACKAGE\\s*BODY\\s*(\\S*\\s)?(IS|AS)");
    private static final StatementType PLSQL_PACKAGE_BODY_STATEMENT = new StatementType();
    private static final Pattern PLSQL_PACKAGE_DEFINITION_REGEX = Pattern.compile(
        "^CREATE(\\s*OR\\s*REPLACE)?(\\s*(NON)?EDITIONABLE)?\\s*PACKAGE\\s([^\\s*]*\\s*)?(AUTHID\\s*[^\\s*]*\\s*|ACCESSIBLE\\sBY\\s\\(?((FUNCTION|PROCEDURE|PACKAGE|TRIGGER|TYPE)\\s\\S*\\s?+)*\\)?)*(IS|AS)");
    private static final Pattern PLSQL_VIEW_REGEX = Pattern.compile(
        "^CREATE(\\sOR\\sREPLACE)?(\\s(NON)?EDITIONABLE)?\\sVIEW\\s(\\S*\\s)?AS\\sWITH\\s(PROCEDURE|FUNCTION)");
    private static final StatementType PLSQL_VIEW_STATEMENT = new StatementType();
    private static final Pattern PLSQL_REGEX = Pattern.compile(
        "^CREATE(\\sOR\\sREPLACE)?(\\s(NON)?EDITIONABLE)?\\s(FUNCTION|PROCEDURE|TYPE|TRIGGER)");
    private static final Pattern DECLARE_BEGIN_REGEX = Pattern.compile("^DECLARE|BEGIN|WITH");
    private static final StatementType PLSQL_STATEMENT = new StatementType();
    private static final Pattern JAVA_REGEX = Pattern.compile(
        "^CREATE(\\sOR\\sREPLACE)?(\\sAND\\s(RESOLVE|COMPILE))?(\\sNOFORCE)?\\sJAVA\\s(SOURCE|RESOURCE|CLASS)");
    private static final StatementType PLSQL_JAVA_STATEMENT = new StatementType();
    private static final Pattern PLSQL_PACKAGE_BODY_WRAPPED_REGEX = Pattern.compile(
        "^CREATE(\\sOR\\sREPLACE)?(\\s(NON)?EDITIONABLE)?\\sPACKAGE\\sBODY\\sWRAPPED(\\s\\S*)*");
    private static final Pattern PLSQL_PACKAGE_DEFINITION_WRAPPED_REGEX = Pattern.compile(
        "^CREATE(\\sOR\\sREPLACE)?(\\s(NON)?EDITIONABLE)?\\sPACKAGE\\sWRAPPED(\\s\\S*)*");
    private static final Pattern PLSQL_WRAPPED_REGEX = Pattern.compile(
        "^CREATE(\\sOR\\sREPLACE)?(\\s(NON)?EDITIONABLE)?\\s(FUNCTION|PROCEDURE|TYPE)\\sWRAPPED(\\s\\S*)*");
    private static final StatementType PLSQL_WRAPPED_STATEMENT = new StatementType();
    private static final List<String> CONTROL_FLOW_KEYWORDS = Arrays.asList("IF", "LOOP", "CASE");
    private int initialWrappedBlockDepth = -1;

    public DmParser(Configuration configuration, ParsingContext parsingContext) {
        super(configuration, parsingContext, 3);
    }

    @Override
    protected ParsedSqlStatement createStatement(PeekingReader reader, Recorder recorder, int statementPos,
                                                 int statementLine, int statementCol, int nonCommentPartPos,
                                                 int nonCommentPartLine, int nonCommentPartCol,
                                                 StatementType statementType, boolean canExecuteInTransaction,
                                                 Delimiter delimiter, String sql) throws IOException {
        if (PLSQL_VIEW_STATEMENT == statementType) {
            sql = sql.trim();
            if (sql.endsWith(";")) {
                sql = sql.substring(0, sql.length() - 1);
            }
        }

        return super.createStatement(reader, recorder, statementPos, statementLine, statementCol, nonCommentPartPos,
                                     nonCommentPartLine, nonCommentPartCol, statementType, canExecuteInTransaction,
                                     delimiter, sql);
    }

    @Override
    protected StatementType detectStatementType(String simplifiedStatement, ParserContext context) {
        if (!PLSQL_PACKAGE_BODY_WRAPPED_REGEX.matcher(simplifiedStatement).matches()
            && !PLSQL_PACKAGE_DEFINITION_WRAPPED_REGEX.matcher(simplifiedStatement).matches()
            && !PLSQL_WRAPPED_REGEX.matcher(simplifiedStatement).matches()) {
            if (PLSQL_PACKAGE_BODY_REGEX.matcher(simplifiedStatement).matches()) {
                return PLSQL_PACKAGE_BODY_STATEMENT;
            } else if (!PLSQL_REGEX.matcher(simplifiedStatement).matches() && !PLSQL_PACKAGE_DEFINITION_REGEX.matcher(
                simplifiedStatement).matches() && !DECLARE_BEGIN_REGEX.matcher(simplifiedStatement).matches()) {
                if (JAVA_REGEX.matcher(simplifiedStatement).matches()) {
                    return PLSQL_JAVA_STATEMENT;
                } else {
                    return PLSQL_VIEW_REGEX.matcher(simplifiedStatement).matches() ? PLSQL_VIEW_STATEMENT
                        : super.detectStatementType(simplifiedStatement, context);
                }
            } else {
                return PLSQL_STATEMENT;
            }
        } else {
            if (this.initialWrappedBlockDepth == -1) {
                this.initialWrappedBlockDepth = context.getBlockDepth();
            }

            return PLSQL_WRAPPED_STATEMENT;
        }
    }

    @Override
    protected boolean shouldDiscard(Token token, boolean nonCommentPartSeen) {
        return "/".equals(token.getText()) && !nonCommentPartSeen || super.shouldDiscard(token, nonCommentPartSeen);
    }

    @Override
    protected void adjustDelimiter(ParserContext context, StatementType statementType) {
        if (statementType != PLSQL_STATEMENT && statementType != PLSQL_VIEW_STATEMENT
            && statementType != PLSQL_JAVA_STATEMENT && statementType != PLSQL_PACKAGE_BODY_STATEMENT) {
            context.setDelimiter(Delimiter.SEMICOLON);
        } else {
            context.setDelimiter(PLSQL_DELIMITER);
        }

    }

    @Override
    protected boolean shouldAdjustBlockDepth(ParserContext context, List<Token> tokens, Token token) {
        TokenType tokenType = token.getType();
        if (context.getStatementType() != PLSQL_PACKAGE_BODY_STATEMENT || Stream.of(TokenType.EOF, TokenType.DELIMITER)
            .allMatch(i -> i != tokenType)) {
            if (context.getStatementType() == PLSQL_WRAPPED_STATEMENT && Stream.of(TokenType.EOF, TokenType.DELIMITER)
                .anyMatch(i -> i == tokenType)) {
                return true;
            } else {
                return token.getType() == TokenType.SYMBOL && context.getStatementType() == PLSQL_JAVA_STATEMENT
                    || super.shouldAdjustBlockDepth(context, tokens, token);
            }
        } else {
            return true;
        }
    }

    @Override
    protected void adjustBlockDepth(ParserContext context, List<Token> tokens, Token keyword, PeekingReader reader) {
        TokenType tokenType = keyword.getType();
        String keywordText = keyword.getText();
        int parensDepth = keyword.getParensDepth();
        if (!lastTokenIs(tokens, parensDepth, "GOTO")) {
            if (context.getStatementType() == PLSQL_WRAPPED_STATEMENT) {
                if (context.getBlockDepth() == this.initialWrappedBlockDepth) {
                    context.increaseBlockDepth("WRAPPED");
                }

                if (TokenType.EOF == tokenType && context.getBlockDepth() > 0) {
                    context.decreaseBlockDepth();
                }

            } else {
                if (context.getBlockDepth() > this.initialWrappedBlockDepth && "WRAPPED".equals(context.getBlockInitiator())) {
                    this.initialWrappedBlockDepth = -1;
                    context.decreaseBlockDepth();
                }

                if (context.getStatementType() == PLSQL_JAVA_STATEMENT) {
                    if ("{".equals(keywordText)) {
                        context.increaseBlockDepth("PLSQL_JAVA_STATEMENT");
                    } else if ("}".equals(keywordText)) {
                        context.decreaseBlockDepth();
                    }

                } else {
                    if ("BEGIN".equals(keywordText)
                        || CONTROL_FLOW_KEYWORDS.contains(keywordText)
                        && !this.precedingEndAttachesToThisKeyword(tokens, parensDepth, context, keyword)
                        || "TRIGGER".equals(keywordText) && lastTokenIs(tokens, parensDepth, "COMPOUND")
                        || context.getBlockDepth() == 0 && Stream.of(PLSQL_PACKAGE_BODY_REGEX,
                                                                     PLSQL_PACKAGE_DEFINITION_REGEX,
                                                                     PLSQL_TYPE_BODY_REGEX)
                        .anyMatch(i -> this.doTokensMatchPattern(tokens, keyword, i))) {
                        context.increaseBlockDepth(keywordText);
                    } else if ("END".equals(keywordText)) {
                        context.decreaseBlockDepth();
                    }

                    if (context.getBlockDepth() > 0 && lastTokenIs(tokens, parensDepth, "IF")
                        && "EXISTS".equalsIgnoreCase(keywordText)) {
                        context.decreaseBlockDepth();
                    }

                    if (context.getStatementType() == PLSQL_PACKAGE_BODY_STATEMENT && Stream.of(TokenType.EOF,  TokenType.DELIMITER).anyMatch(i -> i == tokenType) && context.getBlockDepth() == 1) {
                        context.decreaseBlockDepth();
                    }

                }
            }
        }
    }

    private boolean precedingEndAttachesToThisKeyword(List<Token> tokens, int parensDepth, ParserContext context,
                                                      Token keyword) {
        return lastTokenIs(tokens, parensDepth, "END")
            && lastTokenIsOnLine(tokens, parensDepth, keyword.getLine())
            && keyword.getText().equals(context.getLastClosedBlockInitiator());
    }

    @Override
    protected boolean doTokensMatchPattern(List<Token> previousTokens, Token current, Pattern regex) {
        if (regex == PLSQL_PACKAGE_DEFINITION_REGEX && previousTokens.stream()
            .anyMatch(t -> t.getType() == TokenType.KEYWORD && "ACCESSIBLE".equalsIgnoreCase(t.getText()))) {
            List<String> tokenStrings = new ArrayList<>();
            tokenStrings.add(current.getText());

            for (int i = previousTokens.size() - 1; i >= 0; --i) {
                Token prevToken = previousTokens.get(i);
                if (prevToken.getType() == TokenType.KEYWORD) {
                    tokenStrings.add(prevToken.getText());
                }
            }

            StringBuilder builder = new StringBuilder();

            for (int i = tokenStrings.size() - 1; i >= 0; --i) {
                builder.append(tokenStrings.get(i));
                if (i != 0) {
                    builder.append(" ");
                }
            }

            return regex.matcher(builder.toString()).matches() || super.doTokensMatchPattern(previousTokens, current, regex);
        } else {
            return super.doTokensMatchPattern(previousTokens, current, regex);
        }
    }

    @Override
    protected boolean isDelimiter(String peek, ParserContext context, int col, int colIgnoringWhitespace) {
        Delimiter delimiter = context.getDelimiter();
        if (peek.startsWith(delimiter.getEscape() + delimiter.getDelimiter())) {
            return true;
        } else {
            if (delimiter.shouldBeAloneOnLine()) {
                if (colIgnoringWhitespace == 1 && peek.equals(delimiter.getDelimiter())) {
                    return true;
                }

                if (colIgnoringWhitespace != 1) {
                    return false;
                }
            } else if (colIgnoringWhitespace == 1 && "/".equals(peek.trim())) {
                return true;
            }

            return super.isDelimiter(peek, context, col, colIgnoringWhitespace);
        }
    }

    @Override
    protected boolean isAlternativeStringLiteral(String peek) {
        if (peek.length() < 3) {
            return false;
        } else {
            char firstChar = peek.charAt(0);
            return Stream.of('q', 'Q').anyMatch(i -> firstChar == i) && peek.charAt(1) == '\'';
        }
    }

    @Override
    protected Token handleAlternativeStringLiteral(PeekingReader reader, ParserContext context, int pos, int line,
                                                   int col) throws IOException {
        reader.swallow(2);
        String closeQuote = this.computeAlternativeCloseQuote((char) reader.read());
        reader.swallowUntilExcluding(closeQuote);
        reader.swallow(closeQuote.length());
        return new Token(TokenType.STRING, pos, line, col, null, null, context.getParensDepth());
    }

    private String computeAlternativeCloseQuote(char specialChar) {
        switch (specialChar) {
            case '!':
                return "!'";
            case '(':
                return ")'";
            case '<':
                return ">'";
            case '[':
                return "]'";
            case '{':
                return "}'";
            default:
                return specialChar + "'";
        }
    }
}
