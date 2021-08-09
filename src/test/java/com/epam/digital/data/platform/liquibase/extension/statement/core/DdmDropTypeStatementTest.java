package com.epam.digital.data.platform.liquibase.extension.statement.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmDropTypeStatementTest {
    @Test
    @DisplayName("Check statement")
    public void check() {
        DdmDropTypeStatement statement = new DdmDropTypeStatement("name");
        Assertions.assertTrue(statement instanceof DdmDropTypeStatement);
    }

}