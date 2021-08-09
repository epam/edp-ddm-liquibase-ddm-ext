package com.epam.digital.data.platform.liquibase.extension.sqlgenerator.core;

import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmReferenceTableStatement;
import liquibase.database.core.MockDatabase;
import liquibase.sql.Sql;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DdmReferenceTableGeneratorTest {
    private DdmReferenceTableGenerator generator;
    private DdmReferenceTableStatement statement;

    @BeforeEach
    void setUp() {
        generator = new DdmReferenceTableGenerator();
        statement = new DdmReferenceTableStatement("name");
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
        assertEquals("SELECT create_reference_table('name')", sqls[0].toSql());
    }

}