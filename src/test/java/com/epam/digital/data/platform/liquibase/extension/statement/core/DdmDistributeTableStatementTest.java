package com.epam.digital.data.platform.liquibase.extension.statement.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmDistributeTableStatementTest {
    @Test
    @DisplayName("Check statement")
    public void check() {
        DdmDistributeTableStatement statement = new DdmDistributeTableStatement("name", "type");
        Assertions.assertTrue(statement instanceof DdmDistributeTableStatement);
    }

}