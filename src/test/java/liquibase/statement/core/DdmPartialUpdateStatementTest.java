package liquibase.statement.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmPartialUpdateStatementTest {

    @Test
    @DisplayName("Check statement")
    public void check() {
        DdmPartialUpdateStatement statement = new DdmPartialUpdateStatement();
        statement.setName("name");
        Assertions.assertTrue(statement instanceof DdmPartialUpdateStatement);
    }
}