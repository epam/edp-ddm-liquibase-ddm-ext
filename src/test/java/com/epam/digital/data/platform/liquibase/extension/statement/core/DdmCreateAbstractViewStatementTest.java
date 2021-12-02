package com.epam.digital.data.platform.liquibase.extension.statement.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmCreateAbstractViewStatementTest {

    @Test
    @DisplayName("Check statement")
    public void check() {
        DdmCreateAbstractViewStatement statement = new DdmCreateAbstractViewStatement("name");
        statement.setName("newName");
        Assertions.assertTrue(statement instanceof DdmCreateAbstractViewStatement);
    }
}