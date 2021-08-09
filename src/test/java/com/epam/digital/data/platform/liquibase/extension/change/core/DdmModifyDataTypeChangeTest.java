package com.epam.digital.data.platform.liquibase.extension.change.core;

import liquibase.database.core.MockDatabase;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.ModifyDataTypeStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmModifyDataTypeChangeTest {
    private DdmModifyDataTypeChange change;

    @BeforeEach
    void setUp() {
        change = new DdmModifyDataTypeChange();
        change.setTableName("table");
        change.setColumnName("column");
        change.setNewDataType("type");
    }

    @Test
    @DisplayName("Validate change")
    void validate() {
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(1, statements.length);
        Assertions.assertTrue(statements[0] instanceof ModifyDataTypeStatement);
    }

    @Test
    @DisplayName("Validate change - historyFlag=false")
    void validateHistoryFlagFalse() {
        change.setHistoryFlag(false);
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(1, statements.length);
        Assertions.assertTrue(statements[0] instanceof ModifyDataTypeStatement);
    }

    @Test
    @DisplayName("Validate change - historyFlag=true")
    void validateHistoryFlagTrue() {
        change.setHistoryFlag(true);
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(2, statements.length);
        Assertions.assertTrue(statements[0] instanceof ModifyDataTypeStatement);
        Assertions.assertTrue(statements[1] instanceof ModifyDataTypeStatement);
    }
}