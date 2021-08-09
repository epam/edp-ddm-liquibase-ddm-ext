package com.epam.digital.data.platform.liquibase.extension.sqlgenerator.core;

import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateSimpleSearchConditionStatement;
import liquibase.database.core.MockDatabase;
import liquibase.sql.Sql;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DdmCreateSimpleSearchConditionGeneratorTest {
    private DdmCreateSimpleSearchConditionGenerator generator;
    private DdmCreateSimpleSearchConditionStatement statement;

    @BeforeEach
    void setUp() {
        generator = new DdmCreateSimpleSearchConditionGenerator();
        statement = new DdmCreateSimpleSearchConditionStatement("name");
    }

    @Test
    @DisplayName("Validate generator")
    public void validateChange() {
        Assertions.assertEquals(0, generator.validate(statement, new MockDatabase(), null).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate SQL")
    public void validateSQL() {
        DdmTableConfig table = new DdmTableConfig("table");
        table.setAlias("alias");
        statement.setTable(table);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT alias.* FROM table AS alias;", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - limit all")
    public void validateSQLLimitAll() {
        DdmTableConfig table = new DdmTableConfig("table");
        table.setAlias("alias");
        statement.setTable(table);

        statement.setLimit("all");

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT alias.* FROM table AS alias;", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - limit 1")
    public void validateSQLLimitOne() {
        DdmTableConfig table = new DdmTableConfig("table");
        table.setAlias("alias");
        statement.setTable(table);

        statement.setLimit("1");

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT alias.* FROM table AS alias;", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - indexing none")
    public void validateSQLIndexingNone() {
        DdmTableConfig table = new DdmTableConfig("table");
        table.setAlias("alias");
        statement.setTable(table);

        statement.setIndexing(false);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT alias.* FROM table AS alias;", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - indexing equal")
    public void validateSQLIndexing() {
        DdmTableConfig table = new DdmTableConfig("table");
        table.setAlias("alias");
        statement.setTable(table);

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        column.setSearchType("equal");
        statement.setSearchColumn(column);

        statement.setIndexing(true);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT alias.* FROM table AS alias;" +
                "\n\n" +
                "CREATE INDEX ix_name_table_column ON table(column);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - indexing contains")
    public void validateSQLIndexingLike() {
        DdmTableConfig table = new DdmTableConfig("table");
        table.setAlias("alias");
        statement.setTable(table);

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        column.setType("text");
        column.setSearchType("contains");
        statement.setSearchColumn(column);

        statement.setIndexing(true);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT alias.* FROM table AS alias;" +
                "\n\n" +
                "CREATE INDEX ix_name_table_column ON table(column text_pattern_ops);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - indexing contains char")
    public void validateSQLIndexingLikeChar() {
        DdmTableConfig table = new DdmTableConfig("table");
        table.setAlias("alias");
        statement.setTable(table);

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        column.setType("char");
        column.setSearchType("contains");
        statement.setSearchColumn(column);

        statement.setIndexing(true);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT alias.* FROM table AS alias;" +
                "\n\n" +
                "CREATE INDEX ix_name_table_column ON table(column bpchar_pattern_ops);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - indexing contains varchar")
    public void validateSQLIndexingLikeVarchar() {
        DdmTableConfig table = new DdmTableConfig("table");
        table.setAlias("alias");
        statement.setTable(table);

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        column.setType("varchar");
        column.setSearchType("contains");
        statement.setSearchColumn(column);

        statement.setIndexing(true);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT alias.* FROM table AS alias;" +
                "\n\n" +
                "CREATE INDEX ix_name_table_column ON table(column varchar_pattern_ops);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - indexing startsWith")
    public void validateSQLIndexingBoth() {
        DdmTableConfig table = new DdmTableConfig("table");
        table.setAlias("alias");
        statement.setTable(table);

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        column.setType("text");
        column.setSearchType("startsWith");
        statement.setSearchColumn(column);

        statement.setIndexing(true);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT alias.* FROM table AS alias;" +
                "\n\n" +
                "CREATE INDEX ix_name_table_column ON table(column text_pattern_ops);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - indexing startsWith char")
    public void validateSQLIndexingBothChar() {
        DdmTableConfig table = new DdmTableConfig("table");
        table.setAlias("alias");
        statement.setTable(table);

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        column.setType("char");
        column.setSearchType("startsWith");
        statement.setSearchColumn(column);

        statement.setIndexing(true);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT alias.* FROM table AS alias;" +
                "\n\n" +
                "CREATE INDEX ix_name_table_column ON table(column bpchar_pattern_ops);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - indexing startsWith varchar")
    public void validateSQLIndexingBothVarchar() {
        DdmTableConfig table = new DdmTableConfig("table");
        table.setAlias("alias");
        statement.setTable(table);

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        column.setType("varchar");
        column.setSearchType("startsWith");
        statement.setSearchColumn(column);

        statement.setIndexing(true);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT alias.* FROM table AS alias;" +
                "\n\n" +
                "CREATE INDEX ix_name_table_column ON table(column varchar_pattern_ops);", sqls[0].toSql());
    }

}