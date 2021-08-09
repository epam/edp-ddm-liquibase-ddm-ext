package com.epam.digital.data.platform.liquibase.extension.sqlgenerator.core;

import liquibase.database.core.MockDatabase;
import liquibase.sql.Sql;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmTruncateLocalDataAfterDistributingTableStatement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DdmTruncateLocalDataAfterDistributingTableGeneratorTest {
    private DdmTruncateLocalDataAfterDistributingTableGenerator generator;
    private DdmTruncateLocalDataAfterDistributingTableStatement statement;

    @BeforeEach
    void setUp() {
        generator = new DdmTruncateLocalDataAfterDistributingTableGenerator();
        statement = new DdmTruncateLocalDataAfterDistributingTableStatement("name");
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
        assertEquals("SELECT truncate_local_data_after_distributing_table('name')", sqls[0].toSql());
    }

}