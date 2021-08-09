package com.epam.digital.data.platform.liquibase.extension.statement.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmTruncateLocalDataAfterDistributingTableStatementTest {
    @Test
    @DisplayName("Check statement")
    public void check() {
        DdmTruncateLocalDataAfterDistributingTableStatement statement = new DdmTruncateLocalDataAfterDistributingTableStatement("name");
        Assertions.assertTrue(statement instanceof DdmTruncateLocalDataAfterDistributingTableStatement);
    }

}