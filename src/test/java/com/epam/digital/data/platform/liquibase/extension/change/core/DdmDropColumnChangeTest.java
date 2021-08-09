package com.epam.digital.data.platform.liquibase.extension.change.core;


import liquibase.database.core.MockDatabase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmDropColumnChangeTest {
    private DdmDropColumnChange change;

    @BeforeEach
    void setUp() {
        change = new DdmDropColumnChange();
        change.setTableName("table");
        change.setColumnName("column");
    }

    @Test
    @DisplayName("Validate change")
    void validate() {
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - historyFlag=false")
    void validateHistoryFlagFalse() {
        change.setHistoryFlag(false);
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - historyFlag=true")
    void validateHistoryFlagTrue() {
        change.setHistoryFlag(true);
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }
}