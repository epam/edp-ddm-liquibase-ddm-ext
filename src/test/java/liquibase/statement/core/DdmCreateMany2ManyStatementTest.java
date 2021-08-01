package liquibase.statement.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DdmCreateMany2ManyStatementTest {

    @Test
    @DisplayName("Check statement")
    public void check() {
        DdmCreateMany2ManyStatement statement = new DdmCreateMany2ManyStatement();
        Assertions.assertTrue(statement instanceof DdmCreateMany2ManyStatement);
    }

    @Test
    @DisplayName("Check getName")
    public void checkName() {
        DdmCreateMany2ManyStatement statement = new DdmCreateMany2ManyStatement();
        statement.setMainTableName("mainTable");
        statement.setReferenceTableName("referenceTable");
        assertEquals("mainTable_referenceTable", statement.getName());
    }

    @Test
    @DisplayName("Check Relation")
    public void checkRelation() {
        DdmCreateMany2ManyStatement statement = new DdmCreateMany2ManyStatement();
        statement.setMainTableName("mainTable");
        statement.setReferenceTableName("referenceTable");
        assertEquals("mainTable_referenceTable_rel", statement.getRelationName());
    }

    @Test
    @DisplayName("Check View")
    public void checkView() {
        DdmCreateMany2ManyStatement statement = new DdmCreateMany2ManyStatement();
        statement.setMainTableName("mainTable");
        statement.setReferenceTableName("referenceTable");
        assertEquals("mainTable_referenceTable_rel_v", statement.getViewName());
    }

}