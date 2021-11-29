package com.epam.digital.data.platform.liquibase.extension.change.core;

import com.epam.digital.data.platform.liquibase.extension.DdmTest;
import liquibase.database.core.MockDatabase;
import liquibase.statement.SqlStatement;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateMany2ManyStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmCreateMany2ManyChangeTest {
    private DdmCreateMany2ManyChange change;

    @BeforeEach
    void setUp() {
        change = new DdmCreateMany2ManyChange();
    }

    @Test
    @DisplayName("Check statements")
    public void checkStatements() {
        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(1, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmCreateMany2ManyStatement);
    }

    @Test
    @DisplayName("Validate change")
    public void validateChange() {
        change.setMainTableName("mainTable");
        change.setMainTableKeyField("keyField");
        change.setReferenceTableName("referenceTable");
        change.setReferenceKeysArray("keysArray");
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - mainTableName is required")
    public void validateChangeMainTable() {
        change.setMainTableKeyField("keyField");
        change.setReferenceTableName("referenceTable");
        change.setReferenceKeysArray("keysArray");
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - mainTableKeyField is required")
    public void validateChangeKeyField() {
        change.setMainTableName("mainTable");
        change.setReferenceTableName("referenceTable");
        change.setReferenceKeysArray("keysArray");
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - referenceTableName is required")
    public void validateChangeReferenceTable() {
        change.setMainTableName("mainTable");
        change.setMainTableKeyField("keyField");
        change.setReferenceKeysArray("keysArray");
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - referenceKeysArray is required")
    public void validateChangeKeysArray() {
        change.setMainTableName("mainTable");
        change.setMainTableKeyField("keyField");
        change.setReferenceTableName("referenceTable");
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Check load")
    public void checkLoad() throws Exception {
        Assertions.assertEquals(1, DdmTest.loadChangeSets(DdmTest.TEST_CREATE_M2M_FILE_NAME).size());
    }

}