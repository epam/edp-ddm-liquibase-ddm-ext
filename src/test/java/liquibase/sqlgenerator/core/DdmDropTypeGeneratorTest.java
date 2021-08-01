package liquibase.sqlgenerator.core;

import liquibase.database.core.MockDatabase;
import liquibase.sql.Sql;
import liquibase.statement.core.DdmDropTypeStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DdmDropTypeGeneratorTest {
    private DdmDropTypeGenerator generator;

    @BeforeEach
    void setUp() {
        generator = new DdmDropTypeGenerator();
    }

    @Test
    @DisplayName("Validate change")
    public void validateChange() {
        Assertions.assertEquals(0, generator.validate(new DdmDropTypeStatement("name"), new MockDatabase(), null).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate SQL")
    public void validateSQL() {
        Sql[] sqls = generator.generateSql(new DdmDropTypeStatement("name"), new MockDatabase(), null);
        assertEquals("DROP TYPE name;", sqls[0].toSql());
    }
}