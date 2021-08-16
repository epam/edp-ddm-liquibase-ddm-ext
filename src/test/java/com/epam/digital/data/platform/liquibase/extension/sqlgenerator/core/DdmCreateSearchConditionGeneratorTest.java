package com.epam.digital.data.platform.liquibase.extension.sqlgenerator.core;

import java.util.List;

import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmConditionConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmCteConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmFunctionConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmJoinConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import liquibase.database.core.MockDatabase;
import liquibase.sql.Sql;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateSearchConditionStatement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DdmCreateSearchConditionGeneratorTest {
    private DdmCreateSearchConditionGenerator generator;
    private DdmCreateSearchConditionStatement statement;

    @BeforeEach
    void setUp() {
        generator = new DdmCreateSearchConditionGenerator();
        statement = new DdmCreateSearchConditionStatement("name");
    }

    @Test
    @DisplayName("Validate generator")
    public void validateChange() {
        assertEquals(0, generator.validate(statement, new MockDatabase(), null).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate SQL")
    public void validateSQL() {
        DdmTableConfig table = new DdmTableConfig("table1");
        table.setAlias("t1");

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column1");
        table.addColumn(column);

        statement.addTable(table);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column1 FROM table1 AS t1;", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - column alias")
    public void validateSQLAlias() {
        DdmTableConfig table = new DdmTableConfig("table1");
        table.setAlias("t1");

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column1");
        column.setAlias("col1");
        table.addColumn(column);

        statement.addTable(table);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column1 AS col1 FROM table1 AS t1;", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - returning")
    public void validateSQLReturning() {
        DdmColumnConfig column;

        DdmTableConfig table = new DdmTableConfig("table1");
        table.setAlias("t1");

        column = new DdmColumnConfig();
        column.setName("column1");
        column.setReturning(true);
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column2");
        column.setReturning(false);
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column3");
        column.setReturning(true);
        table.addColumn(column);

        statement.addTable(table);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column1, t1.column2, t1.column3 FROM table1 AS t1;", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - sorting")
    public void validateSQLSorting() {
        DdmColumnConfig column;

        DdmTableConfig table = new DdmTableConfig("table1");
        table.setAlias("t1");

        column = new DdmColumnConfig();
        column.setName("column1");
        column.setSorting("asc");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column2");
        table.addColumn(column);

        statement.addTable(table);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column1, t1.column2 FROM table1 AS t1 ORDER BY t1.column1;", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - sorting desc")
    public void validateSQLSortingDesc() {
        DdmColumnConfig column;

        DdmTableConfig table = new DdmTableConfig("table1");
        table.setAlias("t1");

        column = new DdmColumnConfig();
        column.setName("column1");
        column.setSorting("desc");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column2");
        table.addColumn(column);

        statement.addTable(table);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column1, t1.column2 FROM table1 AS t1 ORDER BY t1.column1 DESC;", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - limit")
    public void validateSQLLimit() {
        DdmColumnConfig column;

        DdmTableConfig table = new DdmTableConfig("table1");
        table.setAlias("t1");

        column = new DdmColumnConfig();
        column.setName("column1");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column2");
        table.addColumn(column);

        statement.addTable(table);
        statement.setLimit("all");

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column1, t1.column2 FROM table1 AS t1;", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - limit 1")
    public void validateSQLLimitOne() {
        DdmColumnConfig column;

        DdmTableConfig table = new DdmTableConfig("table1");
        table.setAlias("t1");

        column = new DdmColumnConfig();
        column.setName("column1");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column2");
        table.addColumn(column);

        statement.addTable(table);
        statement.setLimit("1");

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column1, t1.column2 FROM table1 AS t1;", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - join")
    public void validateSQLJoin() {
        DdmColumnConfig column;
        DdmTableConfig table;
        DdmJoinConfig join;
        List<DdmTableConfig> tables = new ArrayList<>();
        List<DdmJoinConfig> joins = new ArrayList<>();

        table = new DdmTableConfig("table1");
        table.setAlias("t1");

        column = new DdmColumnConfig();
        column.setName("column11");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column12");
        table.addColumn(column);

        tables.add(table);

        table = new DdmTableConfig("table2");
        table.setAlias("t2");

        column = new DdmColumnConfig();
        column.setName("column21");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column22");
        table.addColumn(column);

        tables.add(table);
        statement.setTables(tables);

        join = new DdmJoinConfig();
        join.setType("inner");
        join.setLeftAlias("t1");
        join.addLeftColumn("column11");
        join.setRightAlias("t2");
        join.addRightColumn("column21");

        joins.add(join);
        statement.setJoins(joins);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11, t1.column12, t2.column21, t2.column22 " +
                "FROM table1 AS t1 INNER JOIN table2 AS t2 ON (t1.column11 = t2.column21);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - join two columns")
    public void validateSQLJoinTwo() {
        DdmColumnConfig column;
        DdmTableConfig table;
        DdmJoinConfig join;

        table = new DdmTableConfig("table1");
        table.setAlias("t1");

        column = new DdmColumnConfig();
        column.setName("column11");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column12");
        table.addColumn(column);

        statement.addTable(table);

        table = new DdmTableConfig("table2");
        table.setAlias("t2");

        column = new DdmColumnConfig();
        column.setName("column21");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column22");
        table.addColumn(column);

        statement.addTable(table);

        join = new DdmJoinConfig();
        join.setType("inner");
        join.setLeftAlias("t1");
        join.addLeftColumn("column11");
        join.addLeftColumn("column12");
        join.setRightAlias("t2");
        join.addRightColumn("column21");
        join.addRightColumn("column22");
        statement.addJoin(join);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11, t1.column12, t2.column21, t2.column22 " +
                "FROM table1 AS t1 INNER JOIN table2 AS t2 ON (t1.column11 = t2.column21) AND (t1.column12 = t2.column22);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - join left")
    public void validateSQLJoinLeft() {
        DdmColumnConfig column;
        DdmTableConfig table;
        DdmJoinConfig join;

        table = new DdmTableConfig("table1");
        table.setAlias("t1");

        column = new DdmColumnConfig();
        column.setName("column11");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column12");
        table.addColumn(column);

        statement.addTable(table);

        table = new DdmTableConfig("table2");
        table.setAlias("t2");

        column = new DdmColumnConfig();
        column.setName("column21");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column22");
        table.addColumn(column);

        statement.addTable(table);

        join = new DdmJoinConfig();
        join.setType("left");
        join.setLeftAlias("t1");
        join.addLeftColumn("column11");
        join.setRightAlias("t2");
        join.addRightColumn("column21");
        statement.addJoin(join);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11, t1.column12, t2.column21, t2.column22 " +
                "FROM table1 AS t1 LEFT JOIN table2 AS t2 ON (t1.column11 = t2.column21);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - indexing columns")
    public void validateSQLIndexingColumns() {
        DdmColumnConfig column;
        DdmTableConfig table;
        DdmJoinConfig join;

        table = new DdmTableConfig("table1");
        table.setAlias("t1");

        column = new DdmColumnConfig();
        column.setName("column11");
        column.setSearchType("equal");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column12");
        column.setSearchType("equal");
        table.addColumn(column);

        statement.addTable(table);

        table = new DdmTableConfig("table2");
        table.setAlias("t2");

        column = new DdmColumnConfig();
        column.setName("column21");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column22");
        column.setSearchType("equal");
        table.addColumn(column);

        statement.addTable(table);

        join = new DdmJoinConfig();
        join.setType("inner");
        join.setLeftAlias("t1");
        join.addLeftColumn("column11");
        join.setRightAlias("t2");
        join.addRightColumn("column21");
        statement.addJoin(join);

        statement.setIndexing(true);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11, t1.column12, t2.column21, t2.column22 " +
            "FROM table1 AS t1 INNER JOIN table2 AS t2 ON (t1.column11 = t2.column21);" +
            "\n\n" +
            "CREATE INDEX IF NOT EXISTS ix_table1__column11 ON table1(column11);" +
            "\n\n" +
            "CREATE INDEX IF NOT EXISTS ix_table1__column12 ON table1(column12);" +
            "\n\n" +
            "CREATE INDEX IF NOT EXISTS ix_table2__column22 ON table2(column22);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - indexing columns false")
    public void validateSQLIndexingColumnsFalse() {
        DdmColumnConfig column;
        DdmTableConfig table;
        DdmJoinConfig join;

        table = new DdmTableConfig("table1");
        table.setAlias("t1");

        column = new DdmColumnConfig();
        column.setName("column11");
        column.setSearchType("equal");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column12");
        column.setSearchType("equal");
        table.addColumn(column);

        statement.addTable(table);

        table = new DdmTableConfig("table2");
        table.setAlias("t2");

        column = new DdmColumnConfig();
        column.setName("column21");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column22");
        column.setSearchType("equal");
        table.addColumn(column);

        statement.addTable(table);

        join = new DdmJoinConfig();
        join.setType("inner");
        join.setLeftAlias("t1");
        join.addLeftColumn("column11");
        join.setRightAlias("t2");
        join.addRightColumn("column21");
        statement.addJoin(join);

        statement.setIndexing(false);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11, t1.column12, t2.column21, t2.column22 " +
            "FROM table1 AS t1 INNER JOIN table2 AS t2 ON (t1.column11 = t2.column21);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - indexing equal")
    public void validateSQLIndexingEqual() {
        DdmColumnConfig column;
        DdmTableConfig table;
        DdmJoinConfig join;

        table = new DdmTableConfig("table1");
        table.setAlias("t1");

        column = new DdmColumnConfig();
        column.setName("column11");
        column.setSearchType("equal");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column12");
        table.addColumn(column);

        statement.addTable(table);

        table = new DdmTableConfig("table2");
        table.setAlias("t2");

        column = new DdmColumnConfig();
        column.setName("column21");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column22");
        column.setSearchType("equal");
        table.addColumn(column);

        statement.addTable(table);

        join = new DdmJoinConfig();
        join.setType("inner");
        join.setLeftAlias("t1");
        join.addLeftColumn("column11");
        join.setRightAlias("t2");
        join.addRightColumn("column21");
        statement.addJoin(join);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11, t1.column12, t2.column21, t2.column22 " +
                "FROM table1 AS t1 INNER JOIN table2 AS t2 ON (t1.column11 = t2.column21);" +
                "\n\n" +
                "CREATE INDEX IF NOT EXISTS ix_table1__column11 ON table1(column11);" +
                "\n\n" +
                "CREATE INDEX IF NOT EXISTS ix_table2__column22 ON table2(column22);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - indexing contains")
    public void validateSQLIndexingLike() {
        DdmColumnConfig column;
        DdmTableConfig table;
        DdmJoinConfig join;

        table = new DdmTableConfig("table1");
        table.setAlias("t1");

        column = new DdmColumnConfig();
        column.setName("column11");
        column.setType("text");
        column.setSearchType("contains");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column12");
        table.addColumn(column);

        statement.addTable(table);

        table = new DdmTableConfig("table2");
        table.setAlias("t2");

        column = new DdmColumnConfig();
        column.setName("column21");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column22");
        column.setType("text");
        column.setSearchType("contains");
        table.addColumn(column);

        statement.addTable(table);

        join = new DdmJoinConfig();
        join.setType("inner");
        join.setLeftAlias("t1");
        join.addLeftColumn("column11");
        join.setRightAlias("t2");
        join.addRightColumn("column21");
        statement.addJoin(join);

        statement.setIndexing(true);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11, t1.column12, t2.column21, t2.column22 " +
                "FROM table1 AS t1 INNER JOIN table2 AS t2 ON (t1.column11 = t2.column21);" +
                "\n\n" +
                "CREATE INDEX IF NOT EXISTS ix_table1__column11 ON table1 USING GIN (column11 gin_trgm_ops);" +
                "\n\n" +
                "CREATE INDEX IF NOT EXISTS ix_table2__column22 ON table2 USING GIN (column22 gin_trgm_ops);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - indexing contains char and varchar")
    public void validateSQLIndexingLikeChar() {
        DdmColumnConfig column;
        DdmTableConfig table;
        DdmJoinConfig join;

        table = new DdmTableConfig("table1");
        table.setAlias("t1");

        column = new DdmColumnConfig();
        column.setName("column11");
        column.setType("char");
        column.setSearchType("contains");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column12");
        table.addColumn(column);

        statement.addTable(table);

        table = new DdmTableConfig("table2");
        table.setAlias("t2");

        column = new DdmColumnConfig();
        column.setName("column21");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column22");
        column.setType("varchar");
        column.setSearchType("contains");
        table.addColumn(column);

        statement.addTable(table);

        join = new DdmJoinConfig();
        join.setType("inner");
        join.setLeftAlias("t1");
        join.addLeftColumn("column11");
        join.setRightAlias("t2");
        join.addRightColumn("column21");
        statement.addJoin(join);

        statement.setIndexing(true);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11, t1.column12, t2.column21, t2.column22 " +
                "FROM table1 AS t1 INNER JOIN table2 AS t2 ON (t1.column11 = t2.column21);" +
                "\n\n" +
                "CREATE INDEX IF NOT EXISTS ix_table1__column11 ON table1 USING GIN (column11 gin_trgm_ops);\n" +
                "\n" +
                "CREATE INDEX IF NOT EXISTS ix_table2__column22 ON table2 USING GIN (column22 gin_trgm_ops);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - indexing startsWith")
    public void validateSQLIndexingBoth() {
        DdmColumnConfig column;
        DdmTableConfig table;
        DdmJoinConfig join;

        table = new DdmTableConfig("table1");
        table.setAlias("t1");

        column = new DdmColumnConfig();
        column.setName("column11");
        column.setType("text");
        column.setSearchType("startsWith");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column12");
        table.addColumn(column);

        statement.addTable(table);

        table = new DdmTableConfig("table2");
        table.setAlias("t2");

        column = new DdmColumnConfig();
        column.setName("column21");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column22");
        column.setType("text");
        column.setSearchType("startsWith");
        table.addColumn(column);

        statement.addTable(table);

        join = new DdmJoinConfig();
        join.setType("inner");
        join.setLeftAlias("t1");
        join.addLeftColumn("column11");
        join.setRightAlias("t2");
        join.addRightColumn("column21");
        statement.addJoin(join);

        statement.setIndexing(true);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11, t1.column12, t2.column21, t2.column22 " +
                "FROM table1 AS t1 INNER JOIN table2 AS t2 ON (t1.column11 = t2.column21);" +
                "\n\n" +
                "CREATE INDEX IF NOT EXISTS ix_table1__column11 ON table1(column11 text_pattern_ops);" +
                "\n\n" +
                "CREATE INDEX IF NOT EXISTS ix_table2__column22 ON table2(column22 text_pattern_ops);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - indexing startsWith char and varchar")
    public void validateSQLIndexingBothChar() {
        DdmColumnConfig column;
        DdmTableConfig table;
        DdmJoinConfig join;

        table = new DdmTableConfig("table1");
        table.setAlias("t1");

        column = new DdmColumnConfig();
        column.setName("column11");
        column.setType("char");
        column.setSearchType("startsWith");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column12");
        table.addColumn(column);

        statement.addTable(table);

        table = new DdmTableConfig("table2");
        table.setAlias("t2");

        column = new DdmColumnConfig();
        column.setName("column21");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column22");
        column.setType("varchar");
        column.setSearchType("startsWith");
        table.addColumn(column);

        statement.addTable(table);

        join = new DdmJoinConfig();
        join.setType("inner");
        join.setLeftAlias("t1");
        join.addLeftColumn("column11");
        join.setRightAlias("t2");
        join.addRightColumn("column21");
        statement.addJoin(join);

        statement.setIndexing(true);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11, t1.column12, t2.column21, t2.column22 " +
                "FROM table1 AS t1 INNER JOIN table2 AS t2 ON (t1.column11 = t2.column21);" +
                "\n\n" +
                "CREATE INDEX IF NOT EXISTS ix_table1__column11 ON table1(column11 bpchar_pattern_ops);\n" +
                "\n" +
                "CREATE INDEX IF NOT EXISTS ix_table2__column22 ON table2(column22 varchar_pattern_ops);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - join conditions")
    public void validateSQLJoinConditions() {
        DdmColumnConfig column;
        DdmTableConfig table;
        DdmJoinConfig join;

        table = new DdmTableConfig("table1");
        table.setAlias("t1");

        column = new DdmColumnConfig();
        column.setName("column11");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column12");
        table.addColumn(column);

        statement.addTable(table);

        table = new DdmTableConfig("table2");
        table.setAlias("t2");

        column = new DdmColumnConfig();
        column.setName("column21");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column22");
        table.addColumn(column);

        statement.addTable(table);

        join = new DdmJoinConfig();
        join.setType("inner");
        join.setLeftAlias("t1");
        join.addLeftColumn("column11");
        join.setRightAlias("t2");
        join.addRightColumn("column21");

        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setLogicOperator("and");
        condition.setTableAlias("t1");
        condition.setColumnName("column12");
        condition.setOperator("eq");
        condition.setValue("1");
        conditions.add(condition);

        condition = new DdmConditionConfig();
        condition.setLogicOperator("or");
        condition.setTableAlias("t2");
        condition.setColumnName("column22");
        condition.setOperator("similar");
        condition.setValue("'{80}'");
        conditions.add(condition);

        join.setConditions(conditions);
        statement.addJoin(join);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11, t1.column12, t2.column21, t2.column22 " +
                "FROM table1 AS t1 " +
                "INNER JOIN table2 AS t2 ON (t1.column11 = t2.column21) and (t1.column12 = 1) or (t2.column22 ~ '{80}');", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - join conditions include both")
    public void validateSQLJoinConditionsIncludeBoth() {
        DdmColumnConfig column;
        DdmTableConfig table;
        DdmJoinConfig join;

        table = new DdmTableConfig("table1");
        table.setAlias("t1");

        column = new DdmColumnConfig();
        column.setName("column11");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column12");
        table.addColumn(column);

        statement.addTable(table);

        table = new DdmTableConfig("table2");
        table.setAlias("t2");

        column = new DdmColumnConfig();
        column.setName("column21");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column22");
        table.addColumn(column);

        statement.addTable(table);

        join = new DdmJoinConfig();
        join.setType("inner");
        join.setLeftAlias("t1");
        join.addLeftColumn("column11");
        join.setRightAlias("t2");
        join.addRightColumn("column21");

        List<DdmConditionConfig> conditionsIncluded = new ArrayList<>();
        DdmConditionConfig conditionIncluded = new DdmConditionConfig();
        conditionIncluded.setLogicOperator("and");
        conditionIncluded.setTableAlias("t2");
        conditionIncluded.setColumnName("column22");
        conditionIncluded.setOperator("similar");
        conditionIncluded.setValue("'{80}'");
        conditionsIncluded.add(conditionIncluded);

        conditionIncluded = new DdmConditionConfig();
        conditionIncluded.setLogicOperator("or");
        conditionIncluded.setTableAlias("t2");
        conditionIncluded.setColumnName("column22");
        conditionIncluded.setOperator("similar");
        conditionIncluded.setValue("'{88}'");
        conditionsIncluded.add(conditionIncluded);

        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setLogicOperator("and");
        condition.setTableAlias("t1");
        condition.setColumnName("column12");
        condition.setOperator("eq");
        condition.setValue("1");
        condition.setConditions(conditionsIncluded);
        conditions.add(condition);

        condition = new DdmConditionConfig();
        condition.setLogicOperator("and");
        condition.setTableAlias("t1");
        condition.setColumnName("column22");
        condition.setOperator("eq");
        condition.setValue("2");
        condition.setConditions(conditionsIncluded);
        conditions.add(condition);

        join.setConditions(conditions);
        statement.addJoin(join);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11, t1.column12, t2.column21, t2.column22 " +
                "FROM table1 AS t1 " +
                "INNER JOIN table2 AS t2 ON (t1.column11 = t2.column21) " +
                "and ((t1.column12 = 1) and ((t2.column22 ~ '{80}') or (t2.column22 ~ '{88}'))) " +
                "and ((t1.column22 = 2) and ((t2.column22 ~ '{80}') or (t2.column22 ~ '{88}')));", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - join conditions include first")
    public void validateSQLJoinConditionsIncludeFirst() {
        DdmColumnConfig column;
        DdmTableConfig table;
        DdmJoinConfig join;

        table = new DdmTableConfig("table1");
        table.setAlias("t1");

        column = new DdmColumnConfig();
        column.setName("column11");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column12");
        table.addColumn(column);

        statement.addTable(table);

        table = new DdmTableConfig("table2");
        table.setAlias("t2");

        column = new DdmColumnConfig();
        column.setName("column21");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column22");
        table.addColumn(column);

        statement.addTable(table);

        join = new DdmJoinConfig();
        join.setType("inner");
        join.setLeftAlias("t1");
        join.addLeftColumn("column11");
        join.setRightAlias("t2");
        join.addRightColumn("column21");

        List<DdmConditionConfig> conditionsIncluded = new ArrayList<>();
        DdmConditionConfig conditionIncluded = new DdmConditionConfig();
        conditionIncluded.setLogicOperator("and");
        conditionIncluded.setTableAlias("t2");
        conditionIncluded.setColumnName("column22");
        conditionIncluded.setOperator("similar");
        conditionIncluded.setValue("'{80}'");
        conditionsIncluded.add(conditionIncluded);

        conditionIncluded = new DdmConditionConfig();
        conditionIncluded.setLogicOperator("or");
        conditionIncluded.setTableAlias("t2");
        conditionIncluded.setColumnName("column22");
        conditionIncluded.setOperator("similar");
        conditionIncluded.setValue("'{88}'");
        conditionsIncluded.add(conditionIncluded);

        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setLogicOperator("and");
        condition.setTableAlias("t1");
        condition.setColumnName("column12");
        condition.setOperator("eq");
        condition.setValue("1");
        condition.setConditions(conditionsIncluded);
        conditions.add(condition);

        condition = new DdmConditionConfig();
        condition.setLogicOperator("and");
        condition.setTableAlias("t1");
        condition.setColumnName("column22");
        condition.setOperator("eq");
        condition.setValue("2");
        conditions.add(condition);

        join.setConditions(conditions);
        statement.addJoin(join);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11, t1.column12, t2.column21, t2.column22 " +
                "FROM table1 AS t1 " +
                "INNER JOIN table2 AS t2 ON (t1.column11 = t2.column21) " +
                "and ((t1.column12 = 1) and ((t2.column22 ~ '{80}') or (t2.column22 ~ '{88}'))) " +
                "and (t1.column22 = 2);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - join conditions include second")
    public void validateSQLJoinConditionsIncludeSecond() {
        DdmColumnConfig column;
        DdmTableConfig table;
        DdmJoinConfig join;

        table = new DdmTableConfig("table1");
        table.setAlias("t1");

        column = new DdmColumnConfig();
        column.setName("column11");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column12");
        table.addColumn(column);

        statement.addTable(table);

        table = new DdmTableConfig("table2");
        table.setAlias("t2");

        column = new DdmColumnConfig();
        column.setName("column21");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column22");
        table.addColumn(column);

        statement.addTable(table);

        join = new DdmJoinConfig();
        join.setType("inner");
        join.setLeftAlias("t1");
        join.addLeftColumn("column11");
        join.setRightAlias("t2");
        join.addRightColumn("column21");

        List<DdmConditionConfig> conditionsIncluded = new ArrayList<>();
        DdmConditionConfig conditionIncluded = new DdmConditionConfig();
        conditionIncluded.setLogicOperator("and");
        conditionIncluded.setTableAlias("t2");
        conditionIncluded.setColumnName("column22");
        conditionIncluded.setOperator("similar");
        conditionIncluded.setValue("'{80}'");
        conditionsIncluded.add(conditionIncluded);

        conditionIncluded = new DdmConditionConfig();
        conditionIncluded.setLogicOperator("or");
        conditionIncluded.setTableAlias("t2");
        conditionIncluded.setColumnName("column22");
        conditionIncluded.setOperator("similar");
        conditionIncluded.setValue("'{88}'");
        conditionsIncluded.add(conditionIncluded);

        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setLogicOperator("and");
        condition.setTableAlias("t1");
        condition.setColumnName("column12");
        condition.setOperator("eq");
        condition.setValue("1");
        conditions.add(condition);

        condition = new DdmConditionConfig();
        condition.setLogicOperator("and");
        condition.setTableAlias("t1");
        condition.setColumnName("column22");
        condition.setOperator("eq");
        condition.setValue("2");
        condition.setConditions(conditionsIncluded);
        conditions.add(condition);

        join.setConditions(conditions);
        statement.addJoin(join);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11, t1.column12, t2.column21, t2.column22 " +
                "FROM table1 AS t1 " +
                "INNER JOIN table2 AS t2 ON (t1.column11 = t2.column21) " +
                "and (t1.column12 = 1) " +
                "and ((t1.column22 = 2) and ((t2.column22 ~ '{80}') or (t2.column22 ~ '{88}')));", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - where conditions")
    public void validateSQLWhereConditions() {
        DdmColumnConfig column;
        DdmTableConfig table;

        table = new DdmTableConfig("table1");
        table.setAlias("t1");

        column = new DdmColumnConfig();
        column.setName("column11");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column12");
        table.addColumn(column);

        statement.addTable(table);

        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setTableAlias("t1");
        condition.setColumnName("column12");
        condition.setOperator("eq");
        condition.setValue("1");
        conditions.add(condition);

        condition = new DdmConditionConfig();
        condition.setLogicOperator("or");
        condition.setTableAlias("t2");
        condition.setColumnName("column22");
        condition.setOperator("similar");
        condition.setValue("'{80}'");
        conditions.add(condition);

        statement.setConditions(conditions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11, t1.column12 " +
                "FROM table1 AS t1 " +
                "WHERE (t1.column12 = 1) or (t2.column22 ~ '{80}');", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - where conditions include both")
    public void validateSQLWhereConditionsIncludeBoth() {
        DdmColumnConfig column;
        DdmTableConfig table;

        table = new DdmTableConfig("table1");
        table.setAlias("t1");

        column = new DdmColumnConfig();
        column.setName("column11");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column12");
        table.addColumn(column);

        statement.addTable(table);

        List<DdmConditionConfig> conditionsIncluded = new ArrayList<>();
        DdmConditionConfig conditionIncluded = new DdmConditionConfig();
        conditionIncluded.setLogicOperator("and");
        conditionIncluded.setTableAlias("t2");
        conditionIncluded.setColumnName("column22");
        conditionIncluded.setOperator("similar");
        conditionIncluded.setValue("'{80}'");
        conditionsIncluded.add(conditionIncluded);

        conditionIncluded = new DdmConditionConfig();
        conditionIncluded.setLogicOperator("or");
        conditionIncluded.setTableAlias("t2");
        conditionIncluded.setColumnName("column22");
        conditionIncluded.setOperator("similar");
        conditionIncluded.setValue("'{88}'");
        conditionsIncluded.add(conditionIncluded);

        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setTableAlias("t1");
        condition.setColumnName("column12");
        condition.setOperator("eq");
        condition.setValue("1");
        condition.setConditions(conditionsIncluded);
        conditions.add(condition);

        condition = new DdmConditionConfig();
        condition.setLogicOperator("and");
        condition.setTableAlias("t1");
        condition.setColumnName("column22");
        condition.setOperator("eq");
        condition.setValue("2");
        condition.setConditions(conditionsIncluded);
        conditions.add(condition);

        statement.setConditions(conditions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11, t1.column12 " +
                "FROM table1 AS t1 " +
                "WHERE " +
                "((t1.column12 = 1) and ((t2.column22 ~ '{80}') or (t2.column22 ~ '{88}'))) " +
                "and " +
                "((t1.column22 = 2) and ((t2.column22 ~ '{80}') or (t2.column22 ~ '{88}')));", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - where conditions include first")
    public void validateSQLWhereConditionsIncludeFirst() {
        DdmColumnConfig column;
        DdmTableConfig table;

        table = new DdmTableConfig("table1");
        table.setAlias("t1");

        column = new DdmColumnConfig();
        column.setName("column11");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column12");
        table.addColumn(column);

        statement.addTable(table);

        List<DdmConditionConfig> conditionsIncluded = new ArrayList<>();
        DdmConditionConfig conditionIncluded = new DdmConditionConfig();
        conditionIncluded.setLogicOperator("and");
        conditionIncluded.setTableAlias("t2");
        conditionIncluded.setColumnName("column22");
        conditionIncluded.setOperator("similar");
        conditionIncluded.setValue("'{80}'");
        conditionsIncluded.add(conditionIncluded);

        conditionIncluded = new DdmConditionConfig();
        conditionIncluded.setLogicOperator("or");
        conditionIncluded.setTableAlias("t2");
        conditionIncluded.setColumnName("column22");
        conditionIncluded.setOperator("similar");
        conditionIncluded.setValue("'{88}'");
        conditionsIncluded.add(conditionIncluded);

        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setTableAlias("t1");
        condition.setColumnName("column12");
        condition.setOperator("eq");
        condition.setValue("1");
        condition.setConditions(conditionsIncluded);
        conditions.add(condition);

        condition = new DdmConditionConfig();
        condition.setLogicOperator("and");
        condition.setTableAlias("t1");
        condition.setColumnName("column22");
        condition.setOperator("eq");
        condition.setValue("2");
        conditions.add(condition);

        statement.setConditions(conditions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11, t1.column12 " +
                "FROM table1 AS t1 " +
                "WHERE " +
                "((t1.column12 = 1) and ((t2.column22 ~ '{80}') or (t2.column22 ~ '{88}'))) " +
                "and " +
                "(t1.column22 = 2);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - where conditions include second")
    public void validateSQLWhereConditionsIncludeSecond() {
        DdmColumnConfig column;
        DdmTableConfig table;

        table = new DdmTableConfig("table1");
        table.setAlias("t1");

        column = new DdmColumnConfig();
        column.setName("column11");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column12");
        table.addColumn(column);

        statement.addTable(table);

        List<DdmConditionConfig> conditionsIncluded = new ArrayList<>();
        DdmConditionConfig conditionIncluded = new DdmConditionConfig();
        conditionIncluded.setLogicOperator("and");
        conditionIncluded.setTableAlias("t2");
        conditionIncluded.setColumnName("column22");
        conditionIncluded.setOperator("similar");
        conditionIncluded.setValue("'{80}'");
        conditionsIncluded.add(conditionIncluded);

        conditionIncluded = new DdmConditionConfig();
        conditionIncluded.setLogicOperator("or");
        conditionIncluded.setTableAlias("t2");
        conditionIncluded.setColumnName("column22");
        conditionIncluded.setOperator("similar");
        conditionIncluded.setValue("'{88}'");
        conditionsIncluded.add(conditionIncluded);

        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setTableAlias("t1");
        condition.setColumnName("column12");
        condition.setOperator("eq");
        condition.setValue("1");
        conditions.add(condition);

        condition = new DdmConditionConfig();
        condition.setLogicOperator("and");
        condition.setTableAlias("t1");
        condition.setColumnName("column22");
        condition.setOperator("eq");
        condition.setValue("2");
        condition.setConditions(conditionsIncluded);
        conditions.add(condition);

        statement.setConditions(conditions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11, t1.column12 " +
                "FROM table1 AS t1 " +
                "WHERE " +
                "(t1.column12 = 1) " +
                "and " +
                "((t1.column22 = 2) and ((t2.column22 ~ '{80}') or (t2.column22 ~ '{88}')));", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - where operator eq")
    public void validateSQLWhereOperatorEQ() {
        DdmColumnConfig column;
        DdmTableConfig table;

        table = new DdmTableConfig("table");
        table.setAlias("t");

        column = new DdmColumnConfig();
        column.setName("column");
        table.addColumn(column);

        statement.addTable(table);

        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setTableAlias("t");
        condition.setColumnName("column");
        condition.setOperator("eq");
        condition.setValue("1");
        conditions.add(condition);

        statement.setConditions(conditions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t.column " +
                "FROM table AS t " +
                "WHERE " +
                "(t.column = 1);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - where operator ne")
    public void validateSQLWhereOperatorNE() {
        DdmColumnConfig column;
        DdmTableConfig table;

        table = new DdmTableConfig("table");
        table.setAlias("t");

        column = new DdmColumnConfig();
        column.setName("column");
        table.addColumn(column);

        statement.addTable(table);

        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setTableAlias("t");
        condition.setColumnName("column");
        condition.setOperator("ne");
        condition.setValue("1");
        conditions.add(condition);

        statement.setConditions(conditions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t.column " +
                "FROM table AS t " +
                "WHERE " +
                "(t.column <> 1);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - where operator gt")
    public void validateSQLWhereOperatorGT() {
        DdmColumnConfig column;
        DdmTableConfig table;

        table = new DdmTableConfig("table");
        table.setAlias("t");

        column = new DdmColumnConfig();
        column.setName("column");
        table.addColumn(column);

        statement.addTable(table);

        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setTableAlias("t");
        condition.setColumnName("column");
        condition.setOperator("gt");
        condition.setValue("1");
        conditions.add(condition);

        statement.setConditions(conditions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t.column " +
                "FROM table AS t " +
                "WHERE " +
                "(t.column > 1);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - where operator ge")
    public void validateSQLWhereOperatorGE() {
        DdmColumnConfig column;
        DdmTableConfig table;

        table = new DdmTableConfig("table");
        table.setAlias("t");

        column = new DdmColumnConfig();
        column.setName("column");
        table.addColumn(column);

        statement.addTable(table);

        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setTableAlias("t");
        condition.setColumnName("column");
        condition.setOperator("ge");
        condition.setValue("1");
        conditions.add(condition);

        statement.setConditions(conditions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t.column " +
                "FROM table AS t " +
                "WHERE " +
                "(t.column >= 1);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - where operator lt")
    public void validateSQLWhereOperatorLT() {
        DdmColumnConfig column;
        DdmTableConfig table;

        table = new DdmTableConfig("table");
        table.setAlias("t");

        column = new DdmColumnConfig();
        column.setName("column");
        table.addColumn(column);

        statement.addTable(table);

        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setTableAlias("t");
        condition.setColumnName("column");
        condition.setOperator("lt");
        condition.setValue("1");
        conditions.add(condition);

        statement.setConditions(conditions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t.column " +
                "FROM table AS t " +
                "WHERE " +
                "(t.column < 1);", sqls[0].toSql());
    }
    @Test
    @DisplayName("Validate SQL - where operator le")
    public void validateSQLWhereOperatorLE() {
        DdmColumnConfig column;
        DdmTableConfig table;

        table = new DdmTableConfig("table");
        table.setAlias("t");

        column = new DdmColumnConfig();
        column.setName("column");
        table.addColumn(column);

        statement.addTable(table);

        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setTableAlias("t");
        condition.setColumnName("column");
        condition.setOperator("le");
        condition.setValue("1");
        conditions.add(condition);

        statement.setConditions(conditions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t.column " +
                "FROM table AS t " +
                "WHERE " +
                "(t.column <= 1);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - where operator in")
    public void validateSQLWhereOperatorIn() {
        DdmColumnConfig column;
        DdmTableConfig table;

        table = new DdmTableConfig("table");
        table.setAlias("t");

        column = new DdmColumnConfig();
        column.setName("column");
        table.addColumn(column);

        statement.addTable(table);

        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setTableAlias("t");
        condition.setColumnName("column");
        condition.setOperator("in");
        condition.setValue("1, 2, 3");
        conditions.add(condition);

        statement.setConditions(conditions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t.column " +
                "FROM table AS t " +
                "WHERE " +
                "(t.column IN (1, 2, 3));", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - where operator notIn")
    public void validateSQLWhereOperatorNotIn() {
        DdmColumnConfig column;
        DdmTableConfig table;

        table = new DdmTableConfig("table");
        table.setAlias("t");

        column = new DdmColumnConfig();
        column.setName("column");
        table.addColumn(column);

        statement.addTable(table);

        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setTableAlias("t");
        condition.setColumnName("column");
        condition.setOperator("notIn");
        condition.setValue("1, 2, 3");
        conditions.add(condition);

        statement.setConditions(conditions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t.column " +
                "FROM table AS t " +
                "WHERE " +
                "(t.column NOT IN (1, 2, 3));", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - where operator isNull true")
    public void validateSQLWhereOperatorIsNull() {
        DdmColumnConfig column;
        DdmTableConfig table;

        table = new DdmTableConfig("table");
        table.setAlias("t");

        column = new DdmColumnConfig();
        column.setName("column");
        table.addColumn(column);

        statement.addTable(table);

        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setTableAlias("t");
        condition.setColumnName("column");
        condition.setOperator("isNull");
        condition.setValue("true");
        conditions.add(condition);

        statement.setConditions(conditions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t.column " +
                "FROM table AS t " +
                "WHERE " +
                "(t.column IS NULL);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - where operator isNull false")
    public void validateSQLWhereOperatorIsNotNull() {
        DdmColumnConfig column;
        DdmTableConfig table;

        table = new DdmTableConfig("table");
        table.setAlias("t");

        column = new DdmColumnConfig();
        column.setName("column");
        table.addColumn(column);

        statement.addTable(table);

        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setTableAlias("t");
        condition.setColumnName("column");
        condition.setOperator("isNull");
        condition.setValue("false");
        conditions.add(condition);

        statement.setConditions(conditions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t.column " +
                "FROM table AS t " +
                "WHERE " +
                "(t.column IS NOT NULL);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - where operator similar")
    public void validateSQLWhereOperatorSimilar() {
        DdmColumnConfig column;
        DdmTableConfig table;

        table = new DdmTableConfig("table");
        table.setAlias("t");

        column = new DdmColumnConfig();
        column.setName("column");
        table.addColumn(column);

        statement.addTable(table);

        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setTableAlias("t");
        condition.setColumnName("column");
        condition.setOperator("similar");
        condition.setValue("'{80}'");
        conditions.add(condition);

        statement.setConditions(conditions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t.column " +
                "FROM table AS t " +
                "WHERE " +
                "(t.column ~ '{80}');", sqls[0].toSql());
    }
    @Test
    @DisplayName("Validate SQL - where operator like")
    public void validateSQLWhereOperatorLike() {
        DdmColumnConfig column;
        DdmTableConfig table;

        table = new DdmTableConfig("table");
        table.setAlias("t");

        column = new DdmColumnConfig();
        column.setName("column");
        table.addColumn(column);

        statement.addTable(table);

        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setTableAlias("t");
        condition.setColumnName("column");
        condition.setOperator("like");
        condition.setValue("'name%'");
        conditions.add(condition);

        statement.setConditions(conditions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t.column " +
                "FROM table AS t " +
                "WHERE " +
                "(t.column LIKE 'name%');", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - functions")
    public void validateSQLFunctions() {
        DdmColumnConfig column;
        DdmTableConfig table;

        table = new DdmTableConfig("table");
        table.setAlias("t");

        column = new DdmColumnConfig();
        column.setName("column");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column2");
        table.addColumn(column);

        List<DdmFunctionConfig> functions = new ArrayList<>();
        DdmFunctionConfig function = new DdmFunctionConfig();
        function.setTableAlias("t");
        function.setColumnName("column");
        function.setName("count");
        function.setAlias("cnt");
        functions.add(function);

        table.setFunctions(functions);
        statement.addTable(table);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t.column2, COUNT(t.column) AS cnt FROM table AS t GROUP BY t.column2;", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - functions with parameter")
    public void validateSQLFunctionsParameter() {
        DdmColumnConfig column;
        DdmTableConfig table;

        table = new DdmTableConfig("table");
        table.setAlias("t");

        column = new DdmColumnConfig();
        column.setName("column");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column2");
        table.addColumn(column);

        List<DdmFunctionConfig> functions = new ArrayList<>();
        DdmFunctionConfig function = new DdmFunctionConfig();
        function.setTableAlias("t");
        function.setColumnName("column");
        function.setName("string_agg");
        function.setAlias("aggregated");
        function.setParameter("','");
        functions.add(function);

        table.setFunctions(functions);
        statement.addTable(table);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t.column2, STRING_AGG(t.column, ',') AS aggregated FROM table AS t GROUP BY t.column2;", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - CTE")
    public void validateSQLCte() {
        DdmCteConfig cte;
        DdmColumnConfig column;
        DdmTableConfig table;

        table = new DdmTableConfig("table");
        table.setAlias("t");

        column = new DdmColumnConfig();
        column.setName("column");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column2");
        table.addColumn(column);

        List<DdmFunctionConfig> functions = new ArrayList<>();
        DdmFunctionConfig function = new DdmFunctionConfig();
        function.setTableAlias("t");
        function.setColumnName("column");
        function.setName("count");
        function.setAlias("cnt");
        functions.add(function);

        table.setFunctions(functions);

        List<DdmCteConfig> ctes = new ArrayList<>();
        cte = new DdmCteConfig();
        cte.setName("cte_table");
        cte.addTable(table);
        ctes.add(cte);

        statement.setCtes(ctes);

        table = new DdmTableConfig("cte_table");
        table.setAlias("ct");

        column = new DdmColumnConfig();
        column.setName("cnt");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column2");
        table.addColumn(column);

        statement.addTable(table);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS WITH cte_table AS (SELECT t.column2, COUNT(t.column) AS cnt FROM table AS t GROUP BY t.column2) SELECT ct.cnt, ct.column2 FROM cte_table AS ct;", sqls[0].toSql());
    }
}