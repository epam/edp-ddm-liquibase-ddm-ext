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

import com.epam.digital.data.platform.liquibase.extension.DdmResourceAccessor;
import com.epam.digital.data.platform.liquibase.extension.DdmTest;
import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmDistributeTableStatement;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmReferenceTableStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.RuntimeEnvironment;
import liquibase.change.Change;
import liquibase.change.ConstraintsConfig;
import liquibase.change.core.DropTableChange;
import liquibase.changelog.ChangeLogIterator;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.changelog.filter.ChangeSetFilterResult;
import liquibase.changelog.visitor.ChangeSetVisitor;
import liquibase.database.Database;
import liquibase.database.core.MockDatabase;
import liquibase.database.core.PostgresDatabase;
import liquibase.exception.ChangeLogParseException;
import liquibase.exception.LiquibaseException;
import liquibase.parser.core.xml.XMLChangeLogSAXParser;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.AddDefaultValueStatement;
import liquibase.statement.core.CreateIndexStatement;
import liquibase.statement.core.CreateTableStatement;
import liquibase.statement.core.DropPrimaryKeyStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.statement.core.SetColumnRemarksStatement;
import liquibase.statement.core.SetTableRemarksStatement;
import liquibase.structure.core.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DdmCreateTableChangeTest {
    private DdmCreateTableChange change;
    private ChangeLogParameters changeLogParameters;

    @BeforeEach
    void setUp() {
        DatabaseChangeLog changeLog = new DatabaseChangeLog("path");

        changeLogParameters = new ChangeLogParameters();
        changeLog.setChangeLogParameters(changeLogParameters);

        ChangeSet changeSet = new ChangeSet(changeLog);

        change = new DdmCreateTableChange();
        change.setTableName("table");

        changeSet.addChange(change);
        changeLog.addChangeSet(changeSet);

        DdmColumnConfig column1 = new DdmColumnConfig();
        column1.setName("column1");
        column1.setType("type1");
        ConstraintsConfig constraint = new ConstraintsConfig();
        constraint.setPrimaryKey("true");
        column1.setConstraints(constraint);
        change.addColumn(column1);

        DdmColumnConfig column2 = new DdmColumnConfig();
        column2.setName("column2");
        column2.setType("type2");
        change.addColumn(column2);
    }

    @Test
    @DisplayName("Validate change")
    public void validateChange() {
        change.setHistoryFlag(true);
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - historyFlag")
    public void validateChangeHistoryFlag() {
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
        change.setHistoryFlag(false);
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - local")
    public void validateChangeLocal() {
        change.setHistoryFlag(true);
        change.setDistribution("local");
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - distributePrimary")
    public void validateChangeDistributePrimary() {
        change.setHistoryFlag(true);
        change.setDistribution("distributePrimary");
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - distributeAll")
    public void validateChangeDistributeAll() {
        change.setHistoryFlag(true);
        change.setDistribution("distributeAll");
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - distributeHistory")
    public void validateChangeDistributeHistory() {
        change.setHistoryFlag(true);
        change.setDistribution("distributeHistory");
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - referencePrimary")
    public void validateChangeReferencePrimary() {
        change.setHistoryFlag(true);
        change.setDistribution("referencePrimary");
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - referenceAll")
    public void validateChangeReferenceAll() {
        change.setHistoryFlag(true);
        change.setDistribution("referenceAll");
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - referenceHistory")
    public void validateChangeReferenceHistory() {
        change.setHistoryFlag(true);
        change.setDistribution("referenceHistory");
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Check statements")
    public void checkStatements() {
        change.setHistoryFlag(true);

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(5, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[3] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[4] instanceof RawSqlStatement);
    }

    @Test
    public void checkStatementsWithReservedWords() {
        Contexts contexts = new Contexts();
        contexts.add("pub");
        changeLogParameters.setContexts(contexts);

        change.setTableName("order");
        change.setHistoryFlag(true);

        DdmColumnConfig column3 = new DdmColumnConfig();
        column3.setName("user");
        column3.setType("type3");
        change.addColumn(column3);

        MockDatabase db = mock(MockDatabase.class);
        when(db.escapeObjectName("order", Table.class)).thenReturn("\"order\"");
        when(db.escapeObjectName("order_hst", Table.class)).thenReturn("order_hst");
        when(db.escapeObjectName("user", Table.class)).thenReturn("\"user\"");
        SqlStatement[] statements = change.generateStatements(db);
        Assertions.assertEquals(7, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof RawSqlStatement);
        Assertions.assertEquals("REVOKE ALL PRIVILEGES ON TABLE order_hst FROM PUBLIC;", ((RawSqlStatement) statements[2]).getSql());
        Assertions.assertTrue(statements[3] instanceof RawSqlStatement);
        Assertions.assertEquals("GRANT SELECT ON order_hst TO application_role;", ((RawSqlStatement) statements[3]).getSql());
        Assertions.assertTrue(statements[4] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[5] instanceof RawSqlStatement);
        Assertions.assertEquals("REVOKE ALL PRIVILEGES ON TABLE \"order\" FROM PUBLIC;", ((RawSqlStatement) statements[5]).getSql());
        Assertions.assertTrue(statements[6] instanceof RawSqlStatement);
        Assertions.assertEquals("GRANT SELECT ON \"order\" TO application_role;", ((RawSqlStatement) statements[6]).getSql());
    }

    @Test
    @DisplayName("Check statements - parameters PUB")
    public void checkStatementsPub() {
        change.setHistoryFlag(true);

        Contexts contexts = new Contexts();
        contexts.add("pub");
        changeLogParameters.setContexts(contexts);

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(7, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof RawSqlStatement);  // drop access
        Assertions.assertTrue(statements[3] instanceof RawSqlStatement);  // grant app_role
        Assertions.assertTrue(statements[4] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[5] instanceof RawSqlStatement);  // drop access
        Assertions.assertTrue(statements[6] instanceof RawSqlStatement);  // grant app_role
    }

    @Test
    @DisplayName("Check statements - parameters SUB")
    public void checkStatementsSub() {
        change.setHistoryFlag(true);

        Contexts contexts = new Contexts();
        contexts.add("sub");
        changeLogParameters.setContexts(contexts);

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(6, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof RawSqlStatement);  // drop access
        Assertions.assertTrue(statements[3] instanceof RawSqlStatement);  // grant hst_role
        Assertions.assertTrue(statements[4] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[5] instanceof RawSqlStatement);  // drop access
    }

    @Test
    @DisplayName("Check statements - parameters PUB and SUB")
    public void checkStatementsPubSub() {
        change.setHistoryFlag(true);

        Contexts contexts = new Contexts();
        contexts.add("pub");
        contexts.add("sub");
        changeLogParameters.setContexts(contexts);

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(8, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof RawSqlStatement);  // drop access
        Assertions.assertTrue(statements[3] instanceof RawSqlStatement);  // grant app_role
        Assertions.assertTrue(statements[4] instanceof RawSqlStatement);  // grant hst_role
        Assertions.assertTrue(statements[5] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[6] instanceof RawSqlStatement);  // drop access
        Assertions.assertTrue(statements[7] instanceof RawSqlStatement);  // grant app_role
    }

    @Test
    @DisplayName("Check statements - isObject")
    public void checkStatementsIsObject() {
        change.setHistoryFlag(true);
        change.setIsObject(true);

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(6, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[3] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[4] instanceof CreateIndexStatement);
        Assertions.assertTrue(statements[5] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check statements - read mode")
    public void checkStatementsAsyncReadMode() {
        change.setHistoryFlag(true);
        change.setReadMode("async");

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(6, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[3] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[4] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[5] instanceof RawSqlStatement); //  readMode metadata
    }

    @Test
    @DisplayName("Check statements - local")
    public void checkStatementsLocal() {
        change.setHistoryFlag(true);
        change.setDistribution("local");

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(5, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[3] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[4] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check statements - distributeAll")
    public void checkStatementsDistributeAll() {
        change.setHistoryFlag(true);
        change.setDistribution("distributeAll");

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(7, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof DdmDistributeTableStatement);
        Assertions.assertTrue(statements[3] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[4] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[5] instanceof DdmDistributeTableStatement);
        Assertions.assertTrue(statements[6] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check statements - distributePrimary")
    public void checkStatementsDistributePrimary() {
        change.setHistoryFlag(true);
        change.setDistribution("distributePrimary");

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(6, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[3] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[4] instanceof DdmDistributeTableStatement);
        Assertions.assertTrue(statements[5] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check statements - distributeHistory")
    public void checkStatementsDistributeHistory() {
        change.setHistoryFlag(true);
        change.setDistribution("distributeHistory");

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(6, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof DdmDistributeTableStatement);
        Assertions.assertTrue(statements[3] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[4] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[5] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check statements - referenceAll")
    public void checkStatementsReferenceAll() {
        change.setHistoryFlag(true);
        change.setDistribution("referenceAll");

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(7, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof DdmReferenceTableStatement);
        Assertions.assertTrue(statements[3] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[4] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[5] instanceof DdmReferenceTableStatement);
        Assertions.assertTrue(statements[6] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check statements - referencePrimary")
    public void checkStatementsReferencePrimary() {
        change.setHistoryFlag(true);
        change.setDistribution("referencePrimary");

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(6, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[3] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[4] instanceof DdmReferenceTableStatement);
        Assertions.assertTrue(statements[5] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check statements - referenceHistory")
    public void checkStatementsReferenceHistory() {
        change.setHistoryFlag(true);
        change.setDistribution("referenceHistory");

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(6, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof DdmReferenceTableStatement);
        Assertions.assertTrue(statements[3] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[4] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[5] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check statements - table remarks")
    public void checkStatementsRemarks() {
        change.setHistoryFlag(true);
        change.setRemarks("remark");

        SqlStatement[] statements = change.generateStatements(new PostgresDatabase());
        Assertions.assertEquals(7, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof SetTableRemarksStatement);
        Assertions.assertTrue(statements[3] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[4] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[5] instanceof SetTableRemarksStatement);
        Assertions.assertTrue(statements[6] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check statements - column remarks")
    public void checkStatementsColumnRemarks() {
        change.setHistoryFlag(true);
        change.getColumns().get(0).setRemarks("remark");

        SqlStatement[] statements = change.generateStatements(new PostgresDatabase());
        Assertions.assertEquals(7, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof SetColumnRemarksStatement);
        Assertions.assertTrue(statements[3] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[4] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[5] instanceof SetColumnRemarksStatement);
        Assertions.assertTrue(statements[6] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check classify")
    public void checkClassify() {
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column3");
        column.setType("type3");
        column.setClassify("private");
        change.addColumn(column);

        change.setHistoryFlag(true);

        SqlStatement[] statements = change.generateStatements(new PostgresDatabase());
        Assertions.assertEquals(7, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[3] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[4] instanceof CreateIndexStatement);
        Assertions.assertTrue(statements[5] instanceof AddDefaultValueStatement);
        Assertions.assertTrue(statements[6] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check inverse")
    public void checkInverse() {
        Change[] changes = change.createInverses();
        Assertions.assertEquals(1, changes.length);
        Assertions.assertTrue(changes[0] instanceof DropTableChange);
    }

    @Test
    @DisplayName("Check inverse - history")
    public void checkInverseHistory() {
        change.setHistoryFlag(true);

        Change[] changes = change.createInverses();
        Assertions.assertEquals(2, changes.length);
        Assertions.assertTrue(changes[0] instanceof DropTableChange);
        Assertions.assertTrue(changes[1] instanceof DropTableChange);
    }

    @Test
    @DisplayName("Check load")
    public void checkLoad() throws ChangeLogParseException, Exception {
        XMLChangeLogSAXParser xmlParser = new XMLChangeLogSAXParser();
        DdmResourceAccessor resourceAccessor = new DdmResourceAccessor();
        DatabaseChangeLog changeLog = xmlParser.parse(DdmTest.TEST_CREATE_TABLE_FILE_NAME,
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
    @DisplayName("Check subject_id has 'not null' constraint")
    public void checkSubjectIdNotNull() {
        change.setHistoryFlag(true);
        change.setIsObject(true);

        SqlStatement[] statements = change.generateStatements(new MockDatabase());

        List<CreateTableStatement> createTableStatements = Stream.of(statements)
            .filter(x -> x instanceof CreateTableStatement)
            .map(x -> (CreateTableStatement) x)
            .collect(Collectors.toList());
        Assertions.assertEquals(2, createTableStatements.size());

        for (CreateTableStatement stm: createTableStatements) {
            Assertions.assertTrue(stm.getNotNullColumns().containsKey("subject_id"));
        }
    }
}