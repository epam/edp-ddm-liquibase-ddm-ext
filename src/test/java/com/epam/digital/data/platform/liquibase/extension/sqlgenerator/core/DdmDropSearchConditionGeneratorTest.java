package com.epam.digital.data.platform.liquibase.extension.sqlgenerator.core;

import liquibase.database.core.MockDatabase;
import liquibase.sql.Sql;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmDropSearchConditionStatement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DdmDropSearchConditionGeneratorTest {
    private DdmDropSearchConditionGenerator generator;

    @BeforeEach
    void setUp() {
        generator = new DdmDropSearchConditionGenerator();
    }

    @Test
    @DisplayName("Validate change")
    public void validateChange() {
        assertEquals(0, generator.validate(new DdmDropSearchConditionStatement("name"), new MockDatabase(), null).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate SQL")
    public void validateSQL() {
        Sql[] sqls = generator.generateSql(new DdmDropSearchConditionStatement("name"), new MockDatabase(), null);
        assertEquals("drop view if exists name_v;" +
                "\n\n" +
                "delete from ddm_liquibase_metadata where (change_type = 'searchCondition') and (change_name = 'name');" +
                "\n\n" +
                "do $$  declare    txt text; begin  select    string_agg('drop index if exists ' || indexname, '; ') || ';'  into txt  from pg_indexes  where indexname like 'ix_$name$_%';   if txt is not null then    execute txt;  end if; end; $$", sqls[0].toSql());
    }
}