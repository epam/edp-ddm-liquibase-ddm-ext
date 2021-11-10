package com.epam.digital.data.platform.liquibase.extension.change.core;

import liquibase.database.core.MockDatabase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmRenameColumnChangeTest {
    private DdmRenameColumnChange change;

    @BeforeEach
    void setUp() {
        change = new DdmRenameColumnChange();
        change.setTableName("table");
        change.setOldColumnName("oldColumn");
        change.setNewColumnName("newColumn");
    }

    @Test
    @DisplayName("Validate change")
    void validate() {
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - historyFlag=false")
    void validateHistoryFlagFalse() {
        change.setHistoryFlag(false);
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - historyFlag=true")
    void validateHistoryFlagTrue() {
        change.setHistoryFlag(true);
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }
}