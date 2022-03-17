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

import com.epam.digital.data.platform.liquibase.extension.DdmTest;
import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmFunctionConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmJoinConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateAbstractViewStatement;
import java.util.Collections;
import liquibase.Contexts;
import com.epam.digital.data.platform.liquibase.extension.DdmResourceAccessor;
import liquibase.LabelExpression;
import liquibase.RuntimeEnvironment;
import liquibase.change.AddColumnConfig;
import liquibase.change.Change;
import liquibase.changelog.ChangeLogIterator;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.changelog.filter.ChangeSetFilterResult;
import liquibase.changelog.visitor.ChangeSetVisitor;
import liquibase.database.Database;
import liquibase.database.core.MockDatabase;
import liquibase.exception.ChangeLogParseException;
import liquibase.exception.LiquibaseException;
import liquibase.parser.core.xml.XMLChangeLogSAXParser;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

class DdmCreateSearchConditionChangeTest {
    private DdmCreateSearchConditionChange change;
    private ChangeLogParameters changeLogParameters;
    private ChangeSet changeSet;

    @BeforeEach
    void setUp() {
        DatabaseChangeLog changeLog = new DatabaseChangeLog("path");

        changeLogParameters = new ChangeLogParameters();
        changeLog.setChangeLogParameters(changeLogParameters);

        changeSet = new ChangeSet(changeLog);

        change = new DdmCreateSearchConditionChange();

        changeSet.addChange(change);
        changeLog.addChangeSet(changeSet);
    }

    @Test
    @DisplayName("Check statements")
    public void checkStatements() {
        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(2, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmCreateAbstractViewStatement);
        Assertions.assertTrue(statements[1] instanceof RawSqlStatement);  //  grant select to view
    }

    @Test
    @DisplayName("Check statements - read mode")
    public void checkStatementsAsyncReadMode() {
        change.setReadMode("async");

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(3, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmCreateAbstractViewStatement);
        Assertions.assertTrue(statements[1] instanceof RawSqlStatement);  //  grant select to view
        Assertions.assertTrue(statements[2] instanceof RawSqlStatement);  //  readMode metadata
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

        scTable.addColumn(column);
        change.setTables(Collections.singletonList(scTable));

        DdmCreateTableChange tableChange = new DdmCreateTableChange();
        tableChange.setTableName("table");

        DdmColumnConfig column1 = new DdmColumnConfig();
        column1.setName("column_id");
        column1.setType("UUID");

        tableChange.addColumn(column1);
        changeSet.addChange(tableChange);

        change.updateColumnTypes();

        Assertions.assertEquals("uuid", change.getTables().get(0).getColumns().get(0).getType());
    }

    @Test
    public void checkUpdateColumnTypeFromAddColumnChange() {
        change.setName("sc_change");
        DdmTableConfig scTable = new DdmTableConfig();
        scTable.setName("table");

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column_id");
        column.setType("text");

        scTable.addColumn(column);
        change.setTables(Collections.singletonList(scTable));

        DdmAddColumnChange columnChange = new DdmAddColumnChange();
        columnChange.setTableName("table");

        AddColumnConfig column1 = new AddColumnConfig();
        column1.setName("column_id");
        column1.setType("UUID");

        columnChange.addColumn(column1);
        changeSet.addChange(columnChange);

        change.updateColumnTypes();

        Assertions.assertEquals("uuid", change.getTables().get(0).getColumns().get(0).getType());
    }

    @Test
    @DisplayName("Check statements - insert")
    public void checkStatementsInsert() {
        DdmTableConfig table = new DdmTableConfig();
        table.setName("table");
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        column.setReturning(true);
        column.setSearchType("equal");
        table.addColumn(column);
        change.addTable(table);

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(5, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmCreateAbstractViewStatement);
        Assertions.assertTrue(statements[1] instanceof RawSqlStatement);  //  grant select to view
        Assertions.assertTrue(statements[2] instanceof RawSqlStatement);  //  column or alias
        Assertions.assertTrue(statements[3] instanceof RawSqlStatement);  //  mapping column
        Assertions.assertTrue(statements[4] instanceof RawSqlStatement);  //  searchType
    }

    @Test
    @DisplayName("Check statements - function")
    public void checkStatementsFunction() {
        DdmTableConfig table = new DdmTableConfig();
        table.setName("table");
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        column.setReturning(true);
        column.setSearchType("equal");
        DdmFunctionConfig function = new DdmFunctionConfig();
        function.setName("count");
        function.setAlias("cnt");
        function.setColumnName("column");
        function.setWindow("window");
        table.addColumn(column);
        table.addFunction(function);
        change.addTable(table);

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(6, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmCreateAbstractViewStatement);
        Assertions.assertTrue(statements[1] instanceof RawSqlStatement);  //  grant select to view
        Assertions.assertTrue(statements[2] instanceof RawSqlStatement);  //  function
        Assertions.assertTrue(statements[3] instanceof RawSqlStatement);  //  column or alias
        Assertions.assertTrue(statements[4] instanceof RawSqlStatement);  //  mapping column
        Assertions.assertTrue(statements[5] instanceof RawSqlStatement);  //  searchType
    }

    @Test
    @DisplayName("Check statements - limit")
    public void checkStatementsLimit() {
        DdmTableConfig table = new DdmTableConfig("table");
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        column.setSearchType("contains");
        column.setReturning(true);
        table.addColumn(column);
        change.addTable(table);
        change.setLimit("20");

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(6, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmCreateAbstractViewStatement);
        Assertions.assertTrue(statements[1] instanceof RawSqlStatement);  //  grant select to view
        Assertions.assertTrue(statements[2] instanceof RawSqlStatement);  //  column or alias
        Assertions.assertTrue(statements[3] instanceof RawSqlStatement);  //  mapping column
        Assertions.assertTrue(statements[4] instanceof RawSqlStatement);  //  searchType
        Assertions.assertTrue(statements[5] instanceof RawSqlStatement);  //  limit
    }

    @Test
    @DisplayName("Validate change")
    public void validateChange() {
        change.setName("name");
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - name is required")
    public void validateChangeName() {
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - join amount of columns")
    public void validateChangeJoins() {
        change.setName("name");
        DdmJoinConfig join = new DdmJoinConfig();
        join.addLeftColumn("leftColumn");
        join.addRightColumn("rightColumn");
        change.addJoin(join);
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - lists")
    public void validateChangeLists() {
        change.setName("name");
        change.setConditions(new ArrayList<>());
        change.setTables(new ArrayList<>());
        change.setJoins(new ArrayList<>());
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - join amount of left and right columns must be equal")
    public void validateChangeJoinsEqual() {
        change.setName("name");
        DdmJoinConfig join = new DdmJoinConfig();
        join.addLeftColumn("leftColumn");
        change.addJoin(join);
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - search column and type are defined")
    public void validateChangeIndexing() {
        change.setName("name");
        change.setIndexing(true);
        DdmTableConfig table = new DdmTableConfig("table");
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        column.setSearchType("startsWith");
        table.addColumn(column);
        change.addTable(table);
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - search column is not defined")
    public void validateChangeIndexingColumn() {
        DatabaseChangeLog changeLog1 = new DatabaseChangeLog("path");

        ChangeLogParameters changeLogParameters1 = new ChangeLogParameters();
        changeLog1.setChangeLogParameters(changeLogParameters1);

        ChangeSet changeSet1 = new ChangeSet(changeLog1);

        DdmCreateSearchConditionChange change1 = new DdmCreateSearchConditionChange("name");

        changeSet1.addChange(change1);
        changeLog1.addChangeSet(changeSet1);

        change1.setLimit("all");
        change1.setIndexing(true);
        Assertions.assertEquals(1, change1.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - search column type is not defined")
    public void validateChangeIndexingColumnType() {
        change.setName("name");
        change.setIndexing(true);
        DdmTableConfig table = new DdmTableConfig("table");
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        table.addColumn(column);
        change.addTable(table);
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Confirmation Message")
    public void confirmationMessage() {
        change.setName("name");

        Assertions.assertEquals("Search Condition name created", change.getConfirmationMessage());
    }

    @Test
    @DisplayName("Validate change - pagination")
    public void validateChangePagination() {
        change.setName("name");
        change.setPagination(true);
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Check statements - pagination")
    public void checkStatementsPagination() {
        DdmTableConfig table = new DdmTableConfig("table");
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        column.setReturning(true);
        table.addColumn(column);
        change.addTable(table);
        change.setPagination(true);

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(5, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmCreateAbstractViewStatement);
        Assertions.assertTrue(statements[1] instanceof RawSqlStatement);  //  grant select to view
        Assertions.assertTrue(statements[2] instanceof RawSqlStatement);  //  column or alias
        Assertions.assertTrue(statements[3] instanceof RawSqlStatement);  //  mapping column
        Assertions.assertTrue(statements[4] instanceof RawSqlStatement);  //  pagination
    }

    @Test
    @DisplayName("Check load")
    public void checkLoad() throws ChangeLogParseException, Exception {
        XMLChangeLogSAXParser xmlParser = new XMLChangeLogSAXParser();
        DdmResourceAccessor resourceAccessor = new DdmResourceAccessor();
        DatabaseChangeLog changeLog = xmlParser.parse(DdmTest.TEST_CREATE_SEARCH_CONDITION_FILE_NAME,
                new ChangeLogParameters(), resourceAccessor);

        final List<ChangeSet> changeSets = new ArrayList<>();

        new ChangeLogIterator(changeLog).run(new ChangeSetVisitor() {
            @Override
            public Direction getDirection() {
                return Direction.FORWARD;
            }

            @Override
            public void visit(ChangeSet changeSet, DatabaseChangeLog databaseChangeLog, Database database, Set<ChangeSetFilterResult> filterResults) throws LiquibaseException {
                changeSets.add(changeSet);
            }
        }, new RuntimeEnvironment(new MockDatabase(), new Contexts(), new LabelExpression()));

        Assertions.assertEquals(1, changeSets.size());
    }

    @Test
    @DisplayName("Validate inverse")
    public void validateInverse() {
        change.setName("name");
        Change[] changes = change.createInverses();
        changes[0].setChangeSet(change.getChangeSet());
        Assertions.assertEquals(0, changes[0].validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate SQL - functions: parameter is required")
    public void validateSQLFunctionsParameterRequired() {
        change.setName("name");
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
        functions.add(function);

        table.setFunctions(functions);
        change.addTable(table);

        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate SQL - functions: parameter is extra")
    public void validateSQLFunctionsParameterNotRequired() {
        change.setName("name");
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
        function.setParameter("','");
        functions.add(function);

        table.setFunctions(functions);
        change.addTable(table);

        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
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
}