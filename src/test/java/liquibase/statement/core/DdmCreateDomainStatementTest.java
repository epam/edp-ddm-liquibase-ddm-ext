package liquibase.statement.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmCreateDomainStatementTest {
    @Test
    @DisplayName("Check statement")
    public void check() {
        DdmCreateDomainStatement statement1 = new DdmCreateDomainStatement("name");
        Assertions.assertTrue(statement1 instanceof DdmCreateDomainStatement);

        DdmCreateDomainStatement statement2 = new DdmCreateDomainStatement("name", "type");
        Assertions.assertTrue(statement2 instanceof DdmCreateDomainStatement);
    }
}