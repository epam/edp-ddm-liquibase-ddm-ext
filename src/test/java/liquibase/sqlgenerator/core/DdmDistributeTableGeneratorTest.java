package liquibase.sqlgenerator.core;

import liquibase.database.core.MockDatabase;
import liquibase.sql.Sql;
import liquibase.statement.core.DdmCreateDomainStatement;
import liquibase.statement.core.DdmDistributeTableStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DdmDistributeTableGeneratorTest {
    private DdmDistributeTableGenerator generator;
    private DdmDistributeTableStatement statement;

    @BeforeEach
    void setUp() {
        generator = new DdmDistributeTableGenerator();
        statement = new DdmDistributeTableStatement("name", "column");
    }

    @Test
    @DisplayName("Validate change")
    public void validateChange() {
        Assertions.assertEquals(0, generator.validate(statement, new MockDatabase(), null).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate SQL")
    public void validateSQL() {
        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("SELECT create_distributed_table('name', 'column')", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - distributionType")
    public void validateSQLDistributionType() {
        statement.setDistributionType("append");
        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("SELECT create_distributed_table('name', 'column', 'append')", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - colocateWith")
    public void validateSQLColocateWith() {
        statement.setColocateWith("table");
        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("SELECT create_distributed_table('name', 'column', colocate_with=>'table')", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - full")
    public void validateSQLFull() {
        statement.setDistributionType("hash");
        statement.setColocateWith("table");
        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("SELECT create_distributed_table('name', 'column', 'hash', colocate_with=>'table')", sqls[0].toSql());
    }

}