package com.epam.digital.data.platform.liquibase.extension.sqlgenerator.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmDropAnalyticsViewStatement;
import liquibase.database.core.MockDatabase;
import liquibase.sql.Sql;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmDropAnalyticsViewGeneratorTest {
    private DdmDropAnalyticsViewGenerator generator;

    @BeforeEach
    void setUp() {
        generator = new DdmDropAnalyticsViewGenerator();
    }

    @Test
    @DisplayName("Validate change")
    public void validateChange() {
        assertEquals(0, generator.validate(new DdmDropAnalyticsViewStatement("name"), new MockDatabase(), null).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate SQL")
    public void validateSQL() {
        Sql[] sqls = generator.generateSql(new DdmDropAnalyticsViewStatement("name"), new MockDatabase(), null);
        assertEquals("drop view if exists name_v;", sqls[0].toSql());
    }
}