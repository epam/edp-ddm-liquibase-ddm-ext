package liquibase.statement.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmUndistributeTableStatementTest {
    @Test
    @DisplayName("Check statement")
    public void check() {
        DdmUndistributeTableStatement statement = new DdmUndistributeTableStatement("name");
        Assertions.assertTrue(statement instanceof DdmUndistributeTableStatement);
    }

}