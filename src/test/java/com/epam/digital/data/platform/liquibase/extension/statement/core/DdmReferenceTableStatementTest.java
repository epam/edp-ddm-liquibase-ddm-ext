package com.epam.digital.data.platform.liquibase.extension.statement.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmReferenceTableStatementTest {
    @Test
    @DisplayName("Check statement")
    public void check() {
        DdmReferenceTableStatement statement = new DdmReferenceTableStatement("name");
        Assertions.assertTrue(statement instanceof DdmReferenceTableStatement);
    }

}