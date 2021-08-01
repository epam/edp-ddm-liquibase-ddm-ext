package liquibase.change.core;

import liquibase.change.DdmColumnConfig;
import liquibase.change.DdmTableConfig;
import liquibase.database.core.MockDatabase;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.DdmCreateSimpleSearchConditionStatement;
import liquibase.statement.core.InsertStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DdmCreateSimpleSearchConditionChangeTest {
    private DdmCreateSimpleSearchConditionChange change;

    @BeforeEach
    void setUp() {
        change = new DdmCreateSimpleSearchConditionChange();
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
        Assertions.assertTrue(statements[1] instanceof InsertStatement);
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
        Assertions.assertTrue(statements[1] instanceof InsertStatement);
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
        Assertions.assertTrue(statements[1] instanceof InsertStatement);
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
        Assertions.assertTrue(statements[1] instanceof InsertStatement);
    }

    @Test
    @DisplayName("Validate change")
    public void validateChange() {
        change = new DdmCreateSimpleSearchConditionChange("name");

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        change.setSearchColumn(column);

        change.setLimit("all");

        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - index column is required")
    public void validateChangeColumn() {
        change.setName("name");
        change.setIndexing(true);

        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - name is required")
    public void validateChangeName() {
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        change.setSearchColumn(column);

        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - searchType is required")
    public void validateChangeColumnSearchType() {
        change.setName("name");
        change.setIndexing(true);
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        change.setSearchColumn(column);

        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Confirmation Message")
    public void ConfirmationMessage() {
        change.setName("name");

        Assertions.assertEquals("Simple Search Condition name created", change.getConfirmationMessage());
    }
}