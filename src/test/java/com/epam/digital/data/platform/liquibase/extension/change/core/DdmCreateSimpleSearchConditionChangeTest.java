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

package com.epam.digital.data.platform.liquibase.extension.change.core;

import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import liquibase.Contexts;
import liquibase.change.AddColumnConfig;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.core.MockDatabase;
import liquibase.statement.SqlStatement;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateSimpleSearchConditionStatement;
import liquibase.statement.core.RawSqlStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DdmCreateSimpleSearchConditionChangeTest {
    private DdmCreateSimpleSearchConditionChange change;
    private ChangeLogParameters changeLogParameters;
    private ChangeSet changeSet;

    @BeforeEach
    void setUp() {
        change = new DdmCreateSimpleSearchConditionChange();
        DatabaseChangeLog changeLog = new DatabaseChangeLog("path");
        changeSet = new ChangeSet(changeLog);
        changeSet.addChange(change);
        changeLog.addChangeSet(changeSet);

        DdmTableConfig table = new DdmTableConfig("table");
        change.setTable(table);

        changeLogParameters = new ChangeLogParameters();
        changeLog.setChangeLogParameters(changeLogParameters);
    }

    @Test
    @DisplayName("Check ignore")
    public void checkIgnoreChangeSetForContextSub() {
        Contexts contexts = new Contexts();
        contexts.add("sub");
        changeLogParameters.setContexts(contexts);
        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(0, statements.length);
        Assertions.assertTrue(change.getChangeSet().isIgnore());
    }

    @Test
    public void checkUpdateColumnTypeFromCreateTableChange() {
        change.setName("sc_change");
        DdmTableConfig scTable = new DdmTableConfig();
        scTable.setName("table");

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column_id");
        column.setType("text");

        change.setSearchColumn(column);
        change.setTable(scTable);

        DdmCreateTableChange tableChange = new DdmCreateTableChange();
        tableChange.setTableName("table");

        DdmColumnConfig column1 = new DdmColumnConfig();
        column1.setName("column_id");
        column1.setType("UUID");

        tableChange.addColumn(column1);
        changeSet.addChange(tableChange);

        change.updateColumnTypes();

        Assertions.assertEquals("uuid", change.getSearchColumn().getType());
    }

    @Test
    public void checkUpdateColumnTypeFromAddColumnChange() {
        change.setName("sc_change");
        DdmTableConfig scTable = new DdmTableConfig();
        scTable.setName("table");

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column_id");
        column.setType("uuid");

        change.setSearchColumn(column);
        change.setTable(scTable);

        DdmAddColumnChange columnChange = new DdmAddColumnChange();
        columnChange.setTableName("table");

        AddColumnConfig column1 = new AddColumnConfig();
        column1.setName("column_id");
        column1.setType("DN_EDRPOU");

        columnChange.addColumn(column1);
        changeSet.addChange(columnChange);

        change.updateColumnTypes();

        Assertions.assertEquals("dn_edrpou", change.getSearchColumn().getType());
    }

    @Test
    @DisplayName("Check statements - insert")
    public void checkStatementsInsert() {
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        column.setSearchType("equal");
        change.setSearchColumn(column);

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(2, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmCreateSimpleSearchConditionStatement);
        Assertions.assertTrue(statements[1] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check statements - insert contains")
    public void checkStatementsInsertContains() {
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        column.setSearchType("contains");
        change.setSearchColumn(column);

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(2, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmCreateSimpleSearchConditionStatement);
        Assertions.assertTrue(statements[1] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check statements - insert in")
    public void checkStatementsInsertIn() {
        change.setName("change");
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        column.setSearchType("in");
        change.setSearchColumn(column);

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(2, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmCreateSimpleSearchConditionStatement);
        Assertions.assertTrue(statements[1] instanceof RawSqlStatement);
        Assertions.assertEquals("insert into ddm_liquibase_metadata" +
                        "(change_type, change_name, attribute_name, attribute_value) values " +
                        "('searchCondition', 'change', 'inColumn', 'column');\n\n",
                ((RawSqlStatement) statements[1]).getSql());
    }

    @Test
    @DisplayName("Check statements - insert not in")
    public void checkStatementsInsertNotIn() {
        change.setName("change");
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        column.setSearchType("notIn");
        change.setSearchColumn(column);

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(2, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmCreateSimpleSearchConditionStatement);
        Assertions.assertTrue(statements[1] instanceof RawSqlStatement);
        Assertions.assertEquals("insert into ddm_liquibase_metadata" +
                "(change_type, change_name, attribute_name, attribute_value) values " +
                "('searchCondition', 'change', 'notInColumn', 'column');\n\n",
            ((RawSqlStatement) statements[1]).getSql());
    }

    @Test
    @DisplayName("Check statements - insert startsWith")
    public void checkStatementsInsertStartsWith() {
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        column.setSearchType("startsWith");
        change.setSearchColumn(column);

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(2, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmCreateSimpleSearchConditionStatement);
        Assertions.assertTrue(statements[1] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check statements - limit")
    public void checkStatementsLimit() {
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        change.setSearchColumn(column);

        change.setLimit("20");

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(2, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmCreateSimpleSearchConditionStatement);
        Assertions.assertTrue(statements[1] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check statements - read mode")
    public void checkStatementsAsyncReadMode() {
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        column.setSearchType("equal");
        change.setSearchColumn(column);
        change.setReadMode("async");

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(3, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmCreateSimpleSearchConditionStatement);
        Assertions.assertTrue(statements[1] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[2] instanceof RawSqlStatement); //  readMode metadata
    }

    @Test
    @DisplayName("Validate change")
    public void validateChange() {
        change.setName("name");

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        change.setSearchColumn(column);

        change.setLimit("all");

        assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - index column is required")
    public void validateChangeColumn() {
        change.setName("name");
        change.setIndexing(true);

        assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - name is required")
    public void validateChangeName() {
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        change.setSearchColumn(column);

        assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - searchType is required")
    public void validateChangeColumnSearchType() {
        change.setName("name");
        change.setIndexing(true);
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        change.setSearchColumn(column);

        assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - only search condition tags allowed")
    public void validateAllowedTags() {
        change.setName("name");
        DdmCreateAnalyticsViewChange analyticsChange = new DdmCreateAnalyticsViewChange();
        analyticsChange.setName("name");
        changeSet.addChange(analyticsChange);
        changeSet.addChange(change);
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Confirmation Message")
    public void ConfirmationMessage() {
        change.setName("name");

        assertEquals("Simple Search Condition name created", change.getConfirmationMessage());
    }
}