package com.epam.digital.data.platform.liquibase.extension.change.core;

import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmTruncateLocalDataAfterDistributingTableStatement;
import liquibase.database.core.MockDatabase;
import liquibase.statement.SqlStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmTruncateLocalDataAfterDistributingTableChangeTest {
    private DdmTruncateLocalDataAfterDistributingTableChange change;

    @BeforeEach
    void setUp() {
        change = new DdmTruncateLocalDataAfterDistributingTableChange();
    }

    @Test
    @DisplayName("Check statements - scope=all")
    public void checkStatementsAll() {
        change.setScope("all");
        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(2, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmTruncateLocalDataAfterDistributingTableStatement);
        Assertions.assertTrue(statements[1] instanceof DdmTruncateLocalDataAfterDistributingTableStatement);
    }

    @Test
    @DisplayName("Check statements - scope=primary")
    public void checkStatementsPrimary() {
        change.setScope("primary");
        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(1, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmTruncateLocalDataAfterDistributingTableStatement);
    }

    @Test
    @DisplayName("Check statements - scope=history")
    public void checkStatementsHistory() {
        change.setScope("history");
        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(1, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmTruncateLocalDataAfterDistributingTableStatement);
    }

    @Test
    @DisplayName("Validate change")
    public void validateChange() {
        change.setScope("all");
        change.setTableName("name");
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - tableName is required")
    public void validateChangeName() {
        change.setScope("all");
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

}