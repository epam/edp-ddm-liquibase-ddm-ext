package com.epam.digital.data.platform.liquibase.extension.change.core;

import java.util.ArrayList;
import java.util.List;

import com.epam.digital.data.platform.liquibase.extension.DdmTest;
import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmPartialUpdateStatement;
import liquibase.database.core.MockDatabase;
import liquibase.statement.SqlStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmPartialUpdateChangeTest {
    private DdmPartialUpdateChange change;

    @BeforeEach
    void setUp() {
        change = new DdmPartialUpdateChange();
    }

    @Test
    @DisplayName("Validate change")
    public void validateChange() {
        DdmTableConfig table = new DdmTableConfig();
        table.setName("name");

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column1");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column2");
        table.addColumn(column);

        change.addTable(table);

        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - table added twice")
    public void validateChangeTwice() {
        DdmTableConfig table = new DdmTableConfig();
        table.setName("name");

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column1");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column2");
        table.addColumn(column);

        change.addTable(table);
        change.addTable(table);

        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Check load")
    public void checkLoad() throws Exception {
        Assertions.assertEquals(1, DdmTest.loadChangeSets(DdmTest.TEST_PARTIAL_UPDATE_FILE_NAME).size());
    }

    @Test
    @DisplayName("Check statements")
    public void checkStatements() {
        List<DdmTableConfig> tables = new ArrayList<>();
        DdmTableConfig table = new DdmTableConfig("table");

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column1");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column2");
        table.addColumn(column);

        tables.add(table);
        change.setTables(tables);

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(1, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmPartialUpdateStatement);
    }

    @Test
    @DisplayName("Confirmation Message")
    public void confirmationMessage() {
        Assertions.assertEquals("Partial update has been set", change.getConfirmationMessage());
    }
}