package com.epam.digital.data.platform.liquibase.extension.statement.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmCreateSimpleSearchConditionStatementTest {

    @Test
    @DisplayName("Check statement")
    public void check() {
        DdmCreateSimpleSearchConditionStatement statement = new DdmCreateSimpleSearchConditionStatement("name");
        Assertions.assertTrue(statement instanceof DdmCreateSimpleSearchConditionStatement);
    }

}