package com.epam.digital.data.platform.liquibase.extension.sqlgenerator.core;

import liquibase.database.core.MockDatabase;
import liquibase.sql.Sql;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmUndistributeTableStatement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DdmUndistributeTableGeneratorTest {
    private DdmUndistributeTableGenerator generator;
    private DdmUndistributeTableStatement statement;

    @BeforeEach
    void setUp() {
        generator = new DdmUndistributeTableGenerator();
        statement = new DdmUndistributeTableStatement("name");
    }

    @Test
    @DisplayName("Validate change")
    public void validateChange() {
        assertEquals(0, generator.validate(statement, new MockDatabase(), null).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate SQL")
    public void validateSQL() {
        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("SELECT undistribute_table('name')", sqls[0].toSql());
    }

}