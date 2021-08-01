package liquibase.sqlgenerator.core;

import liquibase.database.core.MockDatabase;
import liquibase.sql.Sql;
import liquibase.statement.core.DdmDropDomainStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DdmDropDomainGeneratorTest {
    private DdmDropDomainGenerator generator;

    @BeforeEach
    void setUp() {
        generator = new DdmDropDomainGenerator();
    }

    @Test
    @DisplayName("Validate change")
    public void validateChange() {
        Assertions.assertEquals(0, generator.validate(new DdmDropDomainStatement("name"), new MockDatabase(), null).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate SQL")
    public void validateSQL() {
        Sql[] sqls = generator.generateSql(new DdmDropDomainStatement("name"), new MockDatabase(), null);
        assertEquals("DROP DOMAIN name;", sqls[0].toSql());
    }

}