package liquibase.statement.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DdmDropSearchConditionStatementTest {

    @Test
    @DisplayName("Check statement")
    public void check() {
        DdmDropSearchConditionStatement statement = new DdmDropSearchConditionStatement("name");
        Assertions.assertTrue(statement instanceof DdmDropSearchConditionStatement);
    }
}