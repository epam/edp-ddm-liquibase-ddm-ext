package liquibase.statement.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmCreateSearchConditionStatementTest {

    @Test
    @DisplayName("Check statement")
    public void check() {
        DdmCreateSearchConditionStatement statement = new DdmCreateSearchConditionStatement("name");
        statement.setName("newName");
        Assertions.assertTrue(statement instanceof DdmCreateSearchConditionStatement);
    }
}