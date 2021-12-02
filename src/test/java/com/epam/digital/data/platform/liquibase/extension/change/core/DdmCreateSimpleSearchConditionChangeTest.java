package com.epam.digital.data.platform.liquibase.extension.change.core;

import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import liquibase.Contexts;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.core.MockDatabase;
import liquibase.statement.SqlStatement;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateSimpleSearchConditionStatement;
import liquibase.statement.core.RawSqlStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DdmCreateSimpleSearchConditionChangeTest {
    private DdmCreateSimpleSearchConditionChange change;
    private ChangeLogParameters changeLogParameters;
    private ChangeSet changeSet;

    @BeforeEach
    void setUp() {
        change = new DdmCreateSimpleSearchConditionChange();
        DatabaseChangeLog changeLog = new DatabaseChangeLog("path");
        changeSet = new ChangeSet(changeLog);
        change.setChangeSet(changeSet);

        changeLogParameters = new ChangeLogParameters();
        changeLog.setChangeLogParameters(changeLogParameters);
    }

    @Test
    @DisplayName("Check ignore")
    public void checkIgnoreChangeSetForContextSub() {
        Contexts contexts = new Contexts();
        contexts.add("sub");
        changeLogParameters.setContexts(contexts);
        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(0, statements.length);
        Assertions.assertTrue(change.getChangeSet().isIgnore());
    }

    @Test
    @DisplayName("Check statements - insert")
    public void checkStatementsInsert() {
        DdmTableConfig table = new DdmTableConfig("table");
        change.setTable(table);

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        column.setSearchType("equal");
        change.setSearchColumn(column);

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(2, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmCreateSimpleSearchConditionStatement);
        Assertions.assertTrue(statements[1] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check statements - insert contains")
    public void checkStatementsInsertContains() {
        DdmTableConfig table = new DdmTableConfig("table");
        change.setTable(table);

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        column.setSearchType("contains");
        change.setSearchColumn(column);

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(2, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmCreateSimpleSearchConditionStatement);
        Assertions.assertTrue(statements[1] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check statements - insert startsWith")
    public void checkStatementsInsertStartsWith() {
        DdmTableConfig table = new DdmTableConfig("table");
        change.setTable(table);

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        column.setSearchType("startsWith");
        change.setSearchColumn(column);

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(2, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmCreateSimpleSearchConditionStatement);
        Assertions.assertTrue(statements[1] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check statements - limit")
    public void checkStatementsLimit() {
        DdmTableConfig table = new DdmTableConfig("table");
        change.setTable(table);

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        change.setSearchColumn(column);

        change.setLimit("20");

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(2, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmCreateSimpleSearchConditionStatement);
        Assertions.assertTrue(statements[1] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Validate change")
    public void validateChange() {
        ChangeSet changeSet = change.getChangeSet();
        change = new DdmCreateSimpleSearchConditionChange("name");
        change.setChangeSet(changeSet);

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        change.setSearchColumn(column);

        change.setLimit("all");

        assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - index column is required")
    public void validateChangeColumn() {
        change.setName("name");
        change.setIndexing(true);

        assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - name is required")
    public void validateChangeName() {
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        change.setSearchColumn(column);

        assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - searchType is required")
    public void validateChangeColumnSearchType() {
        change.setName("name");
        change.setIndexing(true);
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        change.setSearchColumn(column);

        assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - only search condition tags allowed")
    public void validateAllowedTags() {
        change.setName("name");
        DdmCreateAnalyticsViewChange analyticsChange = new DdmCreateAnalyticsViewChange();
        analyticsChange.setName("name");
        changeSet.addChange(analyticsChange);
        changeSet.addChange(change);
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Confirmation Message")
    public void ConfirmationMessage() {
        change.setName("name");

        assertEquals("Simple Search Condition name created", change.getConfirmationMessage());
    }
}