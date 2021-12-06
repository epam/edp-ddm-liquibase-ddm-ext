/*
 * Copyright 2021 EPAM Systems.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.epam.digital.data.platform.liquibase.extension.sqlgenerator.core;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmConditionConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmCteConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmFunctionConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmJoinConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateAbstractViewStatement;
import liquibase.database.core.MockDatabase;
import liquibase.sql.Sql;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DdmCreateAbstractViewGeneratorTest {
    private DdmCreateAbstractViewGenerator generator;
    private DdmCreateAbstractViewStatement statement;
    private DdmTableConfig table;
    private DdmColumnConfig column;

    @BeforeEach
    void setUp() {
        generator = new DdmCreateAbstractViewGenerator();
        statement = new DdmCreateAbstractViewStatement("name");
        table = new DdmTableConfig("table1");
        table.setAlias("t1");
        column = new DdmColumnConfig();
        column.setName("column11");
        column.setReturning(true);
        table.addColumn(column);

        statement.addTable(table);
    }

    @Test
    @DisplayName("Validate generator")
    public void validateChange() {
        assertEquals(0, generator.validate(statement, new MockDatabase(), null).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate SQL")
    public void validateSQL() {
        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11 FROM table1 AS t1;", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - column alias")
    public void validateSQLAlias() {
        column.setAlias("col11");

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11 AS col11 FROM table1 AS t1;", sqls[0].toSql());
    }

    @Test
    @Disabled
    @DisplayName("Validate SQL - returning")
    public void validateSQLReturning() {
        column = new DdmColumnConfig();
        column.setName("column2");
        column.setReturning(false);
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column3");
        column.setReturning(true);
        table.addColumn(column);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11, t1.column3 FROM table1 AS t1;", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - sorting")
    public void validateSQLSorting() {
        column.setSorting("asc");

        column = new DdmColumnConfig();
        column.setName("column2");
        column.setReturning(true);
        table.addColumn(column);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11, t1.column2 FROM table1 AS t1 ORDER BY t1.column11;", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - sorting desc")
    public void validateSQLSortingDesc() {
        column.setSorting("desc");

        column = new DdmColumnConfig();
        column.setName("column2");
        column.setReturning(true);
        table.addColumn(column);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11, t1.column2 FROM table1 AS t1 ORDER BY t1.column11 DESC;", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - limit")
    public void validateSQLLimit() {
        column = new DdmColumnConfig();
        column.setName("column2");
        column.setReturning(true);
        table.addColumn(column);

        statement.setLimit("all");

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11, t1.column2 FROM table1 AS t1;", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - limit 1")
    public void validateSQLLimitOne() {
        column = new DdmColumnConfig();
        column.setName("column2");
        column.setReturning(true);
        table.addColumn(column);

        statement.setLimit("1");

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11, t1.column2 FROM table1 AS t1;", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - join")
    public void validateSQLJoin() {
        DdmJoinConfig join;
        List<DdmTableConfig> tables = new ArrayList<>();
        List<DdmJoinConfig> joins = new ArrayList<>();

        column = new DdmColumnConfig();
        column.setName("column12");
        column.setReturning(true);
        table.addColumn(column);

        tables.add(table);

        table = new DdmTableConfig("table2");
        table.setAlias("t2");

        column = new DdmColumnConfig();
        column.setName("column21");
        column.setReturning(true);
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column22");
        column.setReturning(true);
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
        DdmJoinConfig join;

        column = new DdmColumnConfig();
        column.setName("column12");
        column.setReturning(true);
        table.addColumn(column);

        table = new DdmTableConfig("table2");
        table.setAlias("t2");

        column = new DdmColumnConfig();
        column.setName("column21");
        column.setReturning(true);
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column22");
        column.setReturning(true);
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
        DdmJoinConfig join;

        column = new DdmColumnConfig();
        column.setName("column12");
        column.setReturning(true);
        table.addColumn(column);

        table = new DdmTableConfig("table2");
        table.setAlias("t2");

        column = new DdmColumnConfig();
        column.setName("column21");
        column.setReturning(true);
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column22");
        column.setReturning(true);
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
        DdmJoinConfig join;

        column.setSearchType("equal");

        column = new DdmColumnConfig();
        column.setName("column12");
        column.setReturning(true);
        column.setSearchType("equal");
        table.addColumn(column);

        table = new DdmTableConfig("table2");
        table.setAlias("t2");

        column = new DdmColumnConfig();
        column.setName("column21");
        column.setReturning(true);
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column22");
        column.setReturning(true);
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
    void shouldCreateIndexForCteWithRealTableNameAndColumnName() {
        
        // Table 1
        DdmColumnConfig column11 = new DdmColumnConfig();
        column11.setName("c11_name");
        column11.setReturning(true);
        DdmColumnConfig column12 = new DdmColumnConfig();
        column12.setName("c12_name");
        column12.setReturning(true);
        DdmTableConfig table1 = new DdmTableConfig("t1_name");
        table1.setColumns(Arrays.asList(column11, column12));
        
        // Table 1 as part of CTE
        DdmColumnConfig cte_column = new DdmColumnConfig();
        cte_column.setName("c11_name");
        cte_column.setReturning(true);
        cte_column.setAlias("cte_column_alias");
        
        DdmFunctionConfig cte_func = new DdmFunctionConfig();
        cte_func.setName("max");
        cte_func.setColumnName("c12_name");
        cte_func.setAlias("cte_func_alias");

        DdmTableConfig cteTable = new DdmTableConfig();
        cteTable.setName("t1_name");
        cteTable.setColumns(Collections.singletonList(cte_column));
        cteTable.setFunctions(Collections.singletonList(cte_func));

        DdmCteConfig cteConfig = new DdmCteConfig();
        cteConfig.setName("cteName");
        cteConfig.addTable(cteTable);

        // Table 2
        DdmColumnConfig column2 = new DdmColumnConfig();
        column2.setName("c2_name");
        column2.setReturning(true);
        column2.setAlias("c2_alias");

        DdmTableConfig table2 = new DdmTableConfig("t2_name");
        table2.setColumns(Collections.singletonList(column2));
        table2.setAlias("t2_alias");

        // Table 3 - CTE
        DdmColumnConfig column3 = new DdmColumnConfig();
        column3.setName("cte_column_alias");
        column3.setReturning(true);
        column3.setSearchType("contains");

        DdmColumnConfig column4 = new DdmColumnConfig();
        column4.setName("cte_func_alias");
        column4.setReturning(true);
        column4.setSearchType("contains");

        DdmTableConfig table3 = new DdmTableConfig("cteName");
        table3.setColumns(Arrays.asList(column3, column4));
        table3.setAlias("t3_alias");

        DdmJoinConfig join = new DdmJoinConfig();
        join.setType("inner");
        
        join.setLeftAlias("t3_alias");
        join.addLeftColumn("cte_column_alias");
        
        join.setRightAlias("t2_alias");
        join.addRightColumn("c2_alias");

        statement.setName("view_name");
        statement.addJoin(join);
        statement.setCtes(Collections.singletonList(cteConfig));
        statement.setTables(Arrays.asList(table2, table3));
        statement.setIndexing(true);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);

        assertEquals(
            "CREATE OR REPLACE VIEW view_name_v AS " 
                + "WITH cteName AS (SELECT c11_name AS cte_column_alias, MAX(c12_name) " 
                + "AS cte_func_alias FROM t1_name GROUP BY c11_name) SELECT t2_alias.c2_name " 
                + "AS c2_alias, t3_alias.cte_column_alias, t3_alias.cte_func_alias FROM t2_name " 
                + "AS t2_alias INNER JOIN cteName AS t3_alias " 
                + "ON (t3_alias.cte_column_alias = t2_alias.c2_alias);\n"
                + "\n"
                + "CREATE INDEX IF NOT EXISTS ix_t1_name__c11_name " 
                + "ON t1_name USING GIN (c11_name gin_trgm_ops);\n"
                + "\n"
                + "CREATE INDEX IF NOT EXISTS ix_t1_name__c12_name " 
                + "ON t1_name USING GIN (c12_name gin_trgm_ops);",
            sqls[0].toSql());
    }
    
    @Test
    @DisplayName("Validate SQL - indexing equal")
    public void validateSQLIndexingEqual() {
        DdmJoinConfig join;

        column.setSearchType("equal");

        column = new DdmColumnConfig();
        column.setName("column12");
        column.setReturning(true);
        table.addColumn(column);

        table = new DdmTableConfig("table2");
        table.setAlias("t2");

        column = new DdmColumnConfig();
        column.setName("column21");
        column.setReturning(true);
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column22");
        column.setReturning(true);
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
                "CREATE INDEX IF NOT EXISTS ix_table2__column22 ON table2(column22);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - indexing contains")
    public void validateSQLIndexingLike() {
        DdmJoinConfig join;

        column.setType("text");
        column.setSearchType("contains");

        column = new DdmColumnConfig();
        column.setName("column12");
        column.setReturning(true);
        table.addColumn(column);

        table = new DdmTableConfig("table2");
        table.setAlias("t2");

        column = new DdmColumnConfig();
        column.setName("column21");
        column.setReturning(true);
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column22");
        column.setReturning(true);
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
        DdmJoinConfig join;

        column.setType("char");
        column.setSearchType("contains");

        column = new DdmColumnConfig();
        column.setName("column12");
        column.setReturning(true);
        table.addColumn(column);

        table = new DdmTableConfig("table2");
        table.setAlias("t2");

        column = new DdmColumnConfig();
        column.setName("column21");
        column.setReturning(true);
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column22");
        column.setReturning(true);
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
                "CREATE INDEX IF NOT EXISTS ix_table1__column11 ON table1 USING GIN (column11 gin_trgm_ops);" +
                "\n\n" +
                "CREATE INDEX IF NOT EXISTS ix_table2__column22 ON table2 USING GIN (column22 gin_trgm_ops);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - indexing startsWith")
    public void validateSQLIndexingBoth() {
        DdmJoinConfig join;

        column.setType("text");
        column.setSearchType("startsWith");

        column = new DdmColumnConfig();
        column.setName("column12");
        column.setReturning(true);
        table.addColumn(column);

        table = new DdmTableConfig("table2");
        table.setAlias("t2");

        column = new DdmColumnConfig();
        column.setName("column21");
        column.setReturning(true);
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column22");
        column.setReturning(true);
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
        DdmJoinConfig join;

        column.setType("char");
        column.setSearchType("startsWith");

        column = new DdmColumnConfig();
        column.setName("column12");
        column.setReturning(true);
        table.addColumn(column);

        table = new DdmTableConfig("table2");
        table.setAlias("t2");

        column = new DdmColumnConfig();
        column.setName("column21");
        column.setReturning(true);
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column22");
        column.setReturning(true);
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
        DdmJoinConfig join;

        column = new DdmColumnConfig();
        column.setName("column12");
        column.setReturning(true);
        table.addColumn(column);

        table = new DdmTableConfig("table2");
        table.setAlias("t2");

        column = new DdmColumnConfig();
        column.setName("column21");
        column.setReturning(true);
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column22");
        column.setReturning(true);
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
        DdmJoinConfig join;

        column = new DdmColumnConfig();
        column.setName("column12");
        column.setReturning(true);
        table.addColumn(column);

        table = new DdmTableConfig("table2");
        table.setAlias("t2");

        column = new DdmColumnConfig();
        column.setName("column21");
        column.setReturning(true);
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column22");
        column.setReturning(true);
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
        DdmJoinConfig join;

        column = new DdmColumnConfig();
        column.setName("column12");
        column.setReturning(true);
        table.addColumn(column);

        table = new DdmTableConfig("table2");
        table.setAlias("t2");

        column = new DdmColumnConfig();
        column.setName("column21");
        column.setReturning(true);
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column22");
        column.setReturning(true);
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
        DdmJoinConfig join;

        column = new DdmColumnConfig();
        column.setName("column12");
        column.setReturning(true);
        table.addColumn(column);

        table = new DdmTableConfig("table2");
        table.setAlias("t2");

        column = new DdmColumnConfig();
        column.setName("column21");
        column.setReturning(true);
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column22");
        column.setReturning(true);
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
        column = new DdmColumnConfig();
        column.setName("column12");
        column.setReturning(true);
        table.addColumn(column);

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
        column = new DdmColumnConfig();
        column.setName("column12");
        column.setReturning(true);
        table.addColumn(column);

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
        column = new DdmColumnConfig();
        column.setName("column12");
        column.setReturning(true);
        table.addColumn(column);

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
        column = new DdmColumnConfig();
        column.setName("column12");
        column.setReturning(true);
        table.addColumn(column);

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
        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setTableAlias("t1");
        condition.setColumnName("column11");
        condition.setOperator("eq");
        condition.setValue("1");
        conditions.add(condition);

        statement.setConditions(conditions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11 " +
                "FROM table1 AS t1 " +
                "WHERE " +
                "(t1.column11 = 1);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - where operator ne")
    public void validateSQLWhereOperatorNE() {
        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setTableAlias("t1");
        condition.setColumnName("column11");
        condition.setOperator("ne");
        condition.setValue("1");
        conditions.add(condition);

        statement.setConditions(conditions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11 " +
                "FROM table1 AS t1 " +
                "WHERE " +
                "(t1.column11 <> 1);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - where operator gt")
    public void validateSQLWhereOperatorGT() {
        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setTableAlias("t1");
        condition.setColumnName("column11");
        condition.setOperator("gt");
        condition.setValue("1");
        conditions.add(condition);

        statement.setConditions(conditions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11 " +
                "FROM table1 AS t1 " +
                "WHERE " +
                "(t1.column11 > 1);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - where operator ge")
    public void validateSQLWhereOperatorGE() {
        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setTableAlias("t1");
        condition.setColumnName("column11");
        condition.setOperator("ge");
        condition.setValue("1");
        conditions.add(condition);

        statement.setConditions(conditions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11 " +
                "FROM table1 AS t1 " +
                "WHERE " +
                "(t1.column11 >= 1);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - where operator lt")
    public void validateSQLWhereOperatorLT() {
        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setTableAlias("t1");
        condition.setColumnName("column11");
        condition.setOperator("lt");
        condition.setValue("1");
        conditions.add(condition);

        statement.setConditions(conditions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11 " +
                "FROM table1 AS t1 " +
                "WHERE " +
                "(t1.column11 < 1);", sqls[0].toSql());
    }
    @Test
    @DisplayName("Validate SQL - where operator le")
    public void validateSQLWhereOperatorLE() {
        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setTableAlias("t1");
        condition.setColumnName("column11");
        condition.setOperator("le");
        condition.setValue("1");
        conditions.add(condition);

        statement.setConditions(conditions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11 " +
                "FROM table1 AS t1 " +
                "WHERE " +
                "(t1.column11 <= 1);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - where operator in")
    public void validateSQLWhereOperatorIn() {
        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setTableAlias("t1");
        condition.setColumnName("column11");
        condition.setOperator("in");
        condition.setValue("1, 2, 3");
        conditions.add(condition);

        statement.setConditions(conditions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11 " +
                "FROM table1 AS t1 " +
                "WHERE " +
                "(t1.column11 IN (1, 2, 3));", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - where operator notIn")
    public void validateSQLWhereOperatorNotIn() {
        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setTableAlias("t1");
        condition.setColumnName("column11");
        condition.setOperator("notIn");
        condition.setValue("1, 2, 3");
        conditions.add(condition);

        statement.setConditions(conditions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11 " +
                "FROM table1 AS t1 " +
                "WHERE " +
                "(t1.column11 NOT IN (1, 2, 3));", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - where operator isNull true")
    public void validateSQLWhereOperatorIsNull() {
        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setTableAlias("t1");
        condition.setColumnName("column11");
        condition.setOperator("isNull");
        condition.setValue("true");
        conditions.add(condition);

        statement.setConditions(conditions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11 " +
                "FROM table1 AS t1 " +
                "WHERE " +
                "(t1.column11 IS NULL);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - where operator isNull false")
    public void validateSQLWhereOperatorIsNotNull() {
        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setTableAlias("t1");
        condition.setColumnName("column11");
        condition.setOperator("isNull");
        condition.setValue("false");
        conditions.add(condition);

        statement.setConditions(conditions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11 " +
                "FROM table1 AS t1 " +
                "WHERE " +
                "(t1.column11 IS NOT NULL);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - where operator similar")
    public void validateSQLWhereOperatorSimilar() {
        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setTableAlias("t1");
        condition.setColumnName("column11");
        condition.setOperator("similar");
        condition.setValue("'{80}'");
        conditions.add(condition);

        statement.setConditions(conditions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11 " +
                "FROM table1 AS t1 " +
                "WHERE " +
                "(t1.column11 ~ '{80}');", sqls[0].toSql());
    }
    @Test
    @DisplayName("Validate SQL - where operator like")
    public void validateSQLWhereOperatorLike() {
        List<DdmConditionConfig> conditions = new ArrayList<>();
        DdmConditionConfig condition = new DdmConditionConfig();
        condition.setTableAlias("t1");
        condition.setColumnName("column11");
        condition.setOperator("like");
        condition.setValue("'name%'");
        conditions.add(condition);

        statement.setConditions(conditions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column11 " +
                "FROM table1 AS t1 " +
                "WHERE " +
                "(t1.column11 LIKE 'name%');", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - functions")
    public void validateSQLFunctions() {
        column = new DdmColumnConfig();
        column.setName("column2");
        column.setReturning(true);
        table.addColumn(column);

        List<DdmFunctionConfig> functions = new ArrayList<>();
        DdmFunctionConfig function = new DdmFunctionConfig();
        function.setTableAlias("t1");
        function.setColumnName("column11");
        function.setName("count");
        function.setAlias("cnt");
        functions.add(function);

        table.setFunctions(functions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column2, COUNT(t1.column11) AS cnt FROM table1 AS t1 GROUP BY t1.column2;", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - functions with parameter")
    public void validateSQLFunctionsParameter() {
        column = new DdmColumnConfig();
        column.setName("column2");
        column.setReturning(true);
        table.addColumn(column);

        List<DdmFunctionConfig> functions = new ArrayList<>();
        DdmFunctionConfig function = new DdmFunctionConfig();
        function.setTableAlias("t1");
        function.setColumnName("column11");
        function.setName("string_agg");
        function.setAlias("aggregated");
        function.setParameter("','");
        functions.add(function);

        table.setFunctions(functions);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS SELECT t1.column2, STRING_AGG(t1.column11, ',') AS aggregated FROM table1 AS t1 GROUP BY t1.column2;", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - CTE")
    public void validateSQLCte() {
        DdmCteConfig cte;
        DdmCreateAbstractViewStatement statement = new DdmCreateAbstractViewStatement("name");

        column = new DdmColumnConfig();
        column.setName("column2");
        column.setReturning(true);
        table.addColumn(column);

        List<DdmFunctionConfig> functions = new ArrayList<>();
        DdmFunctionConfig function = new DdmFunctionConfig();
        function.setTableAlias("t1");
        function.setColumnName("column11");
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
        column.setReturning(true);
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column2");
        column.setReturning(true);
        table.addColumn(column);

        statement.addTable(table);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW name_v AS WITH cte_table AS (SELECT t1.column2, COUNT(t1.column11) AS cnt FROM table1 AS t1 GROUP BY t1.column2) SELECT ct.cnt, ct.column2 FROM cte_table AS ct;", sqls[0].toSql());
    }
}