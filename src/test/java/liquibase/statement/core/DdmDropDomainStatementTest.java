package liquibase.statement.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmDropDomainStatementTest {
    @Test
    @DisplayName("Check statement")
    public void check() {
        DdmDropDomainStatement statement = new DdmDropDomainStatement("name");
        Assertions.assertTrue(statement instanceof DdmDropDomainStatement);
    }
}