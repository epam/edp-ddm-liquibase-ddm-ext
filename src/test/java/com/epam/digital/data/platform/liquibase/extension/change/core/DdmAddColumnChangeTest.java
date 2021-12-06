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

import com.epam.digital.data.platform.liquibase.extension.DdmMockSnapshotGeneratorFactory;
import liquibase.Contexts;
import liquibase.change.AddColumnConfig;
import liquibase.change.ConstraintsConfig;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.core.MockDatabase;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.AddColumnStatement;
import liquibase.statement.core.AddUniqueConstraintStatement;
import liquibase.statement.core.CreateTableStatement;
import liquibase.statement.core.DropUniqueConstraintStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.statement.core.RenameTableStatement;
import liquibase.structure.core.Column;
import liquibase.structure.core.DataType;
import liquibase.structure.core.Index;
import liquibase.structure.core.Table;
import liquibase.structure.core.UniqueConstraint;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static java.util.Arrays.asList;

class DdmAddColumnChangeTest {
    private DdmAddColumnChange change;
    private DdmAddColumnChange snapshotChange;

    @BeforeEach
    void setUp() {
        DatabaseChangeLog changeLog = new DatabaseChangeLog("path");

        ChangeLogParameters changeLogParameters = new ChangeLogParameters();
        changeLog.setChangeLogParameters(changeLogParameters);

        ChangeSet changeSet = new ChangeSet(changeLog);

        change = new DdmAddColumnChange();
        change.setTableName("table");
        change.setChangeSet(changeSet);

        Table historyTable = new Table();
        historyTable.setName("table_hst");

        DataType colType = new DataType("text");
        Column column = new Column("column");
        column.setNullable(false);
        column.setType(colType);
        historyTable.addColumn(column);

        Index uniqueIndex = new Index();
        uniqueIndex.setName("uniqueConstraint");
        uniqueIndex.setUnique(true);
        uniqueIndex.setColumns(Collections.singletonList(column));

        historyTable.setAttribute("indexes", Collections.singletonList(uniqueIndex));

        snapshotChange = new DdmAddColumnChange(new DdmMockSnapshotGeneratorFactory(historyTable));
        snapshotChange.setTableName("table");
        snapshotChange.setChangeSet(changeSet);
    }

    @Test
    @DisplayName("Validate change")
    void validateChange() {
        change.setHistoryFlag(true);
        AddColumnConfig column1 = new AddColumnConfig();
        column1.setName("column1");
        column1.setType("type1");
        column1.setDefaultValue("defaultValue");
        ConstraintsConfig constraint = new ConstraintsConfig();
        constraint.setNullable("true");
        column1.setConstraints(constraint);
        change.addColumn(column1);

        AddColumnConfig column2 = new AddColumnConfig();
        column2.setName("column2");
        column2.setType("type2");
        change.addColumn(column2);

        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - default value")
    void validateChangeDefaultValue() {
        AddColumnConfig column1 = new AddColumnConfig();
        column1.setName("column1");
        column1.setType("type1");
        ConstraintsConfig constraint = new ConstraintsConfig();
        constraint.setNullable("false");
        column1.setConstraints(constraint);
        change.addColumn(column1);

        AddColumnConfig column2 = new AddColumnConfig();
        column2.setName("column2");
        column2.setType("type2");
        change.addColumn(column2);

        Assertions.assertEquals(3, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Check statements")
    void checkStatements() {
        snapshotChange.setHistoryFlag(true);

        AddColumnConfig column1 = new AddColumnConfig();
        column1.setName("column1");
        column1.setType("type1");
        column1.setDefaultValue("defaultValue");
        ConstraintsConfig constraint = new ConstraintsConfig();
        constraint.setNullable("true");
        column1.setConstraints(constraint);
        snapshotChange.addColumn(column1);

        AddColumnConfig column2 = new AddColumnConfig();
        column2.setName("column2");
        column2.setType("type2");
        snapshotChange.addColumn(column2);

        SqlStatement[] statements = snapshotChange.generateStatements(new MockDatabase());
        Assertions.assertEquals(7, statements.length);
        Assertions.assertTrue(statements[0] instanceof AddColumnStatement);
        Assertions.assertTrue(statements[1] instanceof DropUniqueConstraintStatement);
        Assertions.assertEquals("uniqueConstraint",
                ((DropUniqueConstraintStatement) statements[1]).getConstraintName());
        Assertions.assertTrue(statements[2] instanceof RenameTableStatement);
        Assertions.assertEquals("table_hst",
                ((RenameTableStatement) statements[2]).getOldTableName());
        Assertions.assertEquals("table_hstnull",
                ((RenameTableStatement) statements[2]).getNewTableName());
        Assertions.assertTrue(statements[3] instanceof CreateTableStatement);
        Assertions.assertEquals("table_hst",
                ((CreateTableStatement) statements[3]).getTableName());
        Assertions.assertTrue(statements[4] instanceof RawSqlStatement);
        Assertions.assertEquals("REVOKE ALL PRIVILEGES ON TABLE table_hst FROM PUBLIC;",
                ((RawSqlStatement) statements[4]).getSql());
        Assertions.assertTrue(statements[5] instanceof RawSqlStatement);
        Assertions.assertEquals("CALL p_init_new_hist_table('table_hstnull', 'table_hst');",
                ((RawSqlStatement) statements[5]).getSql());
        Assertions.assertTrue(statements[6] instanceof RawSqlStatement);
        Assertions.assertEquals("ALTER TABLE table_hstnull SET SCHEMA archive;",
                ((RawSqlStatement) statements[6]).getSql());
    }

    @Test
    @DisplayName("Check snapshot")
    void checkSnapshot() {
        snapshotChange.setHistoryFlag(true);

        MockDatabase database = new MockDatabase();

        SqlStatement[] statements = snapshotChange.generateStatements(database);
        Assertions.assertEquals(7, statements.length);
        Assertions.assertTrue(statements[0] instanceof AddColumnStatement);
        Assertions.assertTrue(statements[1] instanceof DropUniqueConstraintStatement);
        Assertions.assertTrue(statements[2] instanceof RenameTableStatement);
        Assertions.assertTrue(statements[3] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[4] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[5] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[6] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check pub access")
    void checkPubAccess() {
        snapshotChange.setHistoryFlag(true);
        snapshotChange.getChangeSet().getChangeLog().getChangeLogParameters().setContexts(new Contexts("pub"));
        MockDatabase database = new MockDatabase();

        SqlStatement[] statements = snapshotChange.generateStatements(database);
        Assertions.assertEquals(8, statements.length);
        Assertions.assertTrue(statements[0] instanceof AddColumnStatement);
        Assertions.assertTrue(statements[1] instanceof DropUniqueConstraintStatement);
        Assertions.assertTrue(statements[2] instanceof RenameTableStatement);
        Assertions.assertTrue(statements[3] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[4] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[5] instanceof RawSqlStatement);
        Assertions.assertEquals("GRANT SELECT ON table_hst TO application_role;",
                ((RawSqlStatement) statements[5]).getSql());
        Assertions.assertTrue(statements[6] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[7] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check sub access")
    void checkSubAccess() {
        snapshotChange.setHistoryFlag(true);
        snapshotChange.getChangeSet().getChangeLog().getChangeLogParameters().setContexts(new Contexts("sub"));
        MockDatabase database = new MockDatabase();

        SqlStatement[] statements = snapshotChange.generateStatements(database);
        Assertions.assertEquals(8, statements.length);
        Assertions.assertTrue(statements[0] instanceof AddColumnStatement);
        Assertions.assertTrue(statements[1] instanceof DropUniqueConstraintStatement);
        Assertions.assertTrue(statements[2] instanceof RenameTableStatement);
        Assertions.assertTrue(statements[3] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[4] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[5] instanceof RawSqlStatement);
        Assertions.assertEquals("GRANT SELECT ON table_hst TO historical_data_role;",
                ((RawSqlStatement) statements[5]).getSql());
        Assertions.assertTrue(statements[6] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[7] instanceof RawSqlStatement);
    }

}