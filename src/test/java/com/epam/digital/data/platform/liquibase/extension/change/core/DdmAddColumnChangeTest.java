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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.DdmMockSnapshotGeneratorFactory;
import com.epam.digital.data.platform.liquibase.extension.change.DdmAddColumnConfig;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import liquibase.Contexts;
import liquibase.change.AddColumnConfig;
import liquibase.change.ConstraintsConfig;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.core.MockDatabase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.statement.ColumnConstraint;
import liquibase.statement.NotNullConstraint;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.AddColumnStatement;
import liquibase.statement.core.CreateTableStatement;
import liquibase.statement.core.DropUniqueConstraintStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.statement.core.RenameTableStatement;
import liquibase.structure.core.Column;
import liquibase.structure.core.DataType;
import liquibase.structure.core.Index;
import liquibase.structure.core.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DdmAddColumnChangeTest {
    private DdmAddColumnChange change;
    private DdmAddColumnChange snapshotChange;
    private DatabaseChangeLog changeLog;
    private ChangeSet changeSet1;
    private ChangeSet changeSet2;
        
    @BeforeEach
    void setUp() {
        changeLog = new DatabaseChangeLog("path");

        ChangeLogParameters changeLogParameters = new ChangeLogParameters();
        changeLog.setChangeLogParameters(changeLogParameters);

        changeSet1 = new ChangeSet("id1", "author1", false, false, "filePath1", "contextList1", "dbmsList1", changeLog);
        changeSet2 = new ChangeSet("id2", "author2", false, false, "filePath2", "contextList2", "dbmsList2", changeLog);

        Table historyTable = new Table();
        historyTable.setName("table_hst");
        
        change = new DdmAddColumnChange(new DdmMockSnapshotGeneratorFactory(historyTable));
        change.setTableName("table");
        change.setHistoryFlag(true);

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
        snapshotChange.setChangeSet(changeSet1);
        snapshotChange.setHistoryFlag(true);
    }

    @Test
    void validateChange() throws SQLException, DatabaseException {
        change.setChangeSet(changeSet2);
        
        ConstraintsConfig constraintsConfig = new ConstraintsConfig();
        constraintsConfig.setNullable(true);

        change.addColumn(mockColumn(constraintsConfig));
        change.addColumn(mockColumn());

        assertEquals(0, change.validate(mockDatabaseWithVersion(null)).getErrorMessages().size());
    }

    @Test
    void validateColumnDefaultValue() throws SQLException, DatabaseException {
        // given
        change.setHistoryFlag(false);
        ConstraintsConfig constraintsConfig = new ConstraintsConfig();
        constraintsConfig.setNullable(false);
        AddColumnConfig column = mockColumn(constraintsConfig);
        column.setDefaultValue(null);
        change.addColumn(column);

        List<String> errorMessages = change.validate(new MockDatabase()).getErrorMessages();

        assertEquals(2, change.validate(mockDatabaseWithVersion(null)).getErrorMessages().size());
        assertEquals("historyFlag attribute is required and must be set as 'true'", errorMessages.get(0));
        assertEquals("Please set default value to not nullable column column1", errorMessages.get(1));
    }

    @Test
    void validateColumnDefaultValueWhenUniqueIsTrue() throws SQLException, DatabaseException {
        // given
        change.setHistoryFlag(false);
        ConstraintsConfig constraintsConfig = new ConstraintsConfig();
        constraintsConfig.setNullable(false);
        constraintsConfig.setUnique(true);
        AddColumnConfig column = mockColumn(constraintsConfig);
        column.setDefaultValue("someString");
        change.addColumn(column);

        // when
        List<String> errorMessages = change.validate(new MockDatabase()).getErrorMessages();

        // then
        assertEquals(2, change.validate(mockDatabaseWithVersion(null)).getErrorMessages().size());
        assertEquals("historyFlag attribute is required and must be set as 'true'", errorMessages.get(0));
        assertEquals("Please choose one - either a default value or a unique one for the column column1", errorMessages.get(1));
    }
    
    @Test
    @DisplayName("Validate column type for auto-generated values")
    public void validateColumnTypeForAutoGeneratedValues() {
        // given
        change.setHistoryFlag(false);
        DdmAddColumnConfig column = new DdmAddColumnConfig();
        column.setName("column3");
        column.setType("type3");
        column.setAutoGenerate("some pattern {DD-MM-YYYY}-{SEQ}");
        change.addColumn(column);

        // when
        List<String> errorMessages = change.validate(new MockDatabase()).getErrorMessages();

        // then
        assertEquals(2, errorMessages.size());
        assertEquals("historyFlag attribute is required and must be set as 'true'", errorMessages.get(0));
        assertEquals(
            "Column 'column3' in table 'table' must be of type TEXT because it stores auto-generated values, but is of type 'type3'",
            errorMessages.get(1));
    }

    @Test
    @DisplayName("Validate pattern for auto-generated values")
    public void validatePatternForAutoGeneratedValues() {
        change.setHistoryFlag(false);
        DdmAddColumnConfig column = new DdmAddColumnConfig();
        column.setName("column3");
        column.setType("TEXT");
        column.setAutoGenerate("some pattern {DffD-MM-YYYY}-{SEQ}");
        change.addColumn(column);

        // when
        List<String> errorMessages = change.validate(new MockDatabase()).getErrorMessages();

        // then
        assertEquals(2, errorMessages.size());
        assertEquals("historyFlag attribute is required and must be set as 'true'", errorMessages.get(0));
        assertEquals(
            "Column 'column3' contains a pattern 'DffD-MM-YYYY' that is not a date/time pattern",
            errorMessages.get(1));
    }

    @Test
    @DisplayName("Validate not applicable pattern for auto-generated values")
    public void validateNotApplicablePatternForAutoGeneratedValues() {
        change.setHistoryFlag(false);
        DdmAddColumnConfig column = new DdmAddColumnConfig();
        column.setName("column3");
        column.setType("TEXT");
        column.setAutoGenerate("some pattern {dd MMMM yyyy zzzz}-{SEQ}");
        change.addColumn(column);

        // when
        List<String> errorMessages = change.validate(new MockDatabase()).getErrorMessages();

        // then
        assertEquals(2, errorMessages.size());
        assertEquals("historyFlag attribute is required and must be set as 'true'", errorMessages.get(0));
        assertEquals(
            "Column 'column3' contains a pattern 'dd MMMM yyyy zzzz' that cannot be applied to date/time formatting",
            errorMessages.get(1));
    }
    
    @Nested
    class RunningChangeLogForTheFirstTimeWhenVersionIsNull {
        
        @Test
        void shouldAlterHstTableAndOriginTable() throws SQLException, DatabaseException {
            // given
            snapshotChange.addColumn(mockColumn());

            // when
            SqlStatement[] statements = snapshotChange.generateStatements(mockDatabaseWithVersion(null));

            // then
            assertEquals(2, statements.length);
            assertTrue(statements[0] instanceof AddColumnStatement);
            assertTrue(statements[1] instanceof AddColumnStatement);
            assertEquals("table", ((AddColumnStatement) statements[0]).getTableName());
            assertEquals("table_hst", ((AddColumnStatement) statements[1]).getTableName());
        }

        @Test
        void onlyNotNullConstraintShouldHaveColumnInHstTable() throws SQLException, DatabaseException {
            // given
            ConstraintsConfig constraintsConfig = new ConstraintsConfig();
            constraintsConfig.setNullable(false);
            constraintsConfig.setPrimaryKey(true);
            snapshotChange.addColumn(mockColumn(constraintsConfig));

            // when
            SqlStatement[] statements = snapshotChange.generateStatements(mockDatabaseWithVersion(null));

            // then
            assertEquals(2, statements.length);
            assertTrue(statements[0] instanceof AddColumnStatement);
            assertTrue(statements[1] instanceof AddColumnStatement);

            assertEquals("table", ((AddColumnStatement) statements[0]).getTableName());
            assertEquals(2, ((AddColumnStatement) statements[0]).getConstraints().size());

            assertEquals("table_hst", ((AddColumnStatement) statements[1]).getTableName());
            Set<ColumnConstraint> constraints = ((AddColumnStatement) statements[1]).getConstraints();
            assertEquals(1, constraints.size());
            assertTrue(constraints.iterator().next() instanceof NotNullConstraint);
        }
    }
    
    @Nested
    class RunningChangeLogNotForTheFirstTimeWhenVersionIsNotNull {
    
        @Test
        void shouldMoveHstTableToArchiveAndCreateNewHstTable() throws SQLException, DatabaseException {
            // given
            snapshotChange.addColumn(mockColumn());

            // when
            SqlStatement[] statements = snapshotChange.generateStatements(mockDatabaseWithVersion("1.0.0"));

            // then
            checkRecreatingAndMovingToArchiveHstTable(statements);
        }

        @Test
        void failValidationWhenHistoryTableWithTheSameVersionIsAlreadyInArchive() throws SQLException, DatabaseException {
            // given
            snapshotChange = new DdmAddColumnChange(new DdmMockSnapshotGeneratorFactory(new Table(null, "archive", "table_hst_1_0_0")));
            snapshotChange.setHistoryFlag(true);
            snapshotChange.setTableName("table");
            snapshotChange.setChangeSet(changeSet1);
            snapshotChange.addColumn(mockColumn());

            // when
            List<String> errorMessages = snapshotChange.validate(mockDatabaseWithVersion("1.0.0")).getErrorMessages();

            // then
            assertEquals(1, errorMessages.size());
            assertEquals("ChangeLog with current version was already ran!", errorMessages.get(0));
        }

        @Test
        void moveHstTableToArchiveForTheFirstChangeAndAlterHstTablesForOtherChangesInTheSameChangeSet() throws SQLException, DatabaseException {
            // given
            changeLog.addChangeSet(changeSet1);
            change.setChangeSet(changeSet1);
                        
            changeSet1.addChange(snapshotChange);
            changeSet1.addChange(change);

            snapshotChange.addColumn(mockColumn());
            change.addColumn(mockColumn());

            // when
            SqlStatement[] statements1 = snapshotChange.generateStatements(mockDatabaseWithVersion("1.0.0"));
            SqlStatement[] statements2 = change.generateStatements(mockDatabaseWithVersion("1.0.0"));

            // then
            checkRecreatingAndMovingToArchiveHstTable(statements1);

            assertEquals(2, statements2.length);
            assertTrue(statements2[0] instanceof AddColumnStatement);
            assertTrue(statements2[1] instanceof AddColumnStatement);
            assertEquals("table", ((AddColumnStatement) statements2[0]).getTableName());
            assertEquals("table_hst", ((AddColumnStatement) statements2[1]).getTableName());
        }

        @Test
        void moveHstTableToArchiveForTheFirstChangeAndAlterHstTablesForOtherChangesInDifferentChangeSets() throws SQLException, DatabaseException {
            // given
            changeLog.addChangeSet(changeSet1);
            changeLog.addChangeSet(changeSet2);
            
            change.setChangeSet(changeSet2);

            changeSet1.addChange(snapshotChange);
            changeSet2.addChange(change);

            snapshotChange.addColumn(mockColumn());
            change.addColumn(mockColumn(new ConstraintsConfig()));

            // when
            SqlStatement[] statements1 = snapshotChange.generateStatements(mockDatabaseWithVersion("1.0.0"));
            SqlStatement[] statements2 = change.generateStatements(mockDatabaseWithVersion("1.0.0"));

            // then
            checkRecreatingAndMovingToArchiveHstTable(statements1);
            assertEquals("column", ((AddColumnStatement) statements1[0]).getColumnName());

            assertEquals(2, statements2.length);
            assertTrue(statements2[0] instanceof AddColumnStatement);
            assertTrue(statements2[1] instanceof AddColumnStatement);
            assertEquals("table", ((AddColumnStatement) statements2[0]).getTableName());
            assertEquals("column1", ((AddColumnStatement) statements2[0]).getColumnName());
            assertEquals("table_hst", ((AddColumnStatement) statements2[1]).getTableName());
        }

        @Test
        @DisplayName("Should generate statements for second ChangeSet, if first one already ran")
        void generateStatementsForSecondChangeSetIfFirstAlreadyRan() throws SQLException, DatabaseException {

            // given
            ChangeSet changeSet3 = new ChangeSet("3", "mock-author", false, false, "test/changelog.xml", "contextList3", "dbmsList3", changeLog);

            changeLog.addChangeSet(changeSet3);
            changeLog.addChangeSet(changeSet2);

            changeSet3.addChange(snapshotChange);
            changeSet2.addChange(change);

            snapshotChange.addColumn(mockColumn());
            change.addColumn(mockColumn());

            // when
            SqlStatement[] statements = change.generateStatements(mockDatabaseWithVersion("1.0.0"));

            // then
            checkRecreatingAndMovingToArchiveHstTable(statements);
        }
    }
    
    @Test
    @DisplayName("Check pub access")
    void checkPubAccess() throws SQLException, DatabaseException {
        // given
        snapshotChange.getChangeSet().getChangeLog().getChangeLogParameters().setContexts(new Contexts("pub"));

        // when
        SqlStatement[] statements = snapshotChange.generateStatements(mockDatabaseWithVersion("1.0.0"));

        // then
        assertEquals(8, statements.length);
        assertTrue(statements[0] instanceof AddColumnStatement);
        assertTrue(statements[1] instanceof DropUniqueConstraintStatement);
        assertTrue(statements[2] instanceof RenameTableStatement);
        assertTrue(statements[3] instanceof CreateTableStatement);
        assertTrue(statements[4] instanceof RawSqlStatement);
        assertTrue(statements[5] instanceof RawSqlStatement);
        assertEquals("GRANT SELECT ON table_hst TO application_role;",
            ((RawSqlStatement) statements[5]).getSql());
        assertTrue(statements[6] instanceof RawSqlStatement);
        assertTrue(statements[7] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check sub access")
    void shouldReturnNullConstraintConfig() {
        // given
        ConstraintsConfig constraintsConfig =
            new ConstraintsConfig()
                .setNullable(true) // .setNullable(false) will fail test
                .setNotNullConstraintName("NotNull")
                .setPrimaryKey(true)
                .setPrimaryKeyName("primaryKey")
                .setPrimaryKeyTablespace("primaryKeyTableSpace")
                .setReferences("references")
                .setUniqueConstraintName("uniqueConstraintName")
                .setUnique(true)
                .setCheckConstraint("checkConstraint")
                .setDeleteCascade(true)
                .setForeignKeyName("foreignKeyName")
                .setInitiallyDeferred(true)
                .setDeferrable(true)
                .setValidateNullable(true)
                .setValidateUnique(true)
                .setValidatePrimaryKey(true)
                .setValidateForeignKey(true);
        constraintsConfig.setReferencedColumnNames("referencedColumnNames");
        constraintsConfig.setReferencedTableCatalogName("referencesCatalogName");
        constraintsConfig.setReferencedTableName("referencesTableName");
        constraintsConfig.setReferencedTableSchemaName("referencesTableSchemaName");

        // when
        AddColumnConfig clone = snapshotChange.retainNotNullConstraint(mockColumn(constraintsConfig));

        // then
        assertNull(clone.getConstraints());
    }
    
    @Test
    @DisplayName("Check sub access")
    void checkSubAccess() throws SQLException, DatabaseException {
        // given
        snapshotChange.getChangeSet().getChangeLog().getChangeLogParameters().setContexts(new Contexts("sub"));

        // when
        SqlStatement[] statements = snapshotChange.generateStatements(mockDatabaseWithVersion("1.0.0"));

        // then
        assertEquals(8, statements.length);
        assertTrue(statements[0] instanceof AddColumnStatement);
        assertTrue(statements[1] instanceof DropUniqueConstraintStatement);
        assertTrue(statements[2] instanceof RenameTableStatement);
        assertTrue(statements[3] instanceof CreateTableStatement);
        assertTrue(statements[4] instanceof RawSqlStatement);
        assertTrue(statements[5] instanceof RawSqlStatement);
        assertEquals("GRANT SELECT ON table_hst TO historical_data_role;",
            ((RawSqlStatement) statements[5]).getSql());
        assertTrue(statements[6] instanceof RawSqlStatement);
        assertTrue(statements[7] instanceof RawSqlStatement);
    }

    private void checkRecreatingAndMovingToArchiveHstTable(SqlStatement[] statements) {
        assertEquals(7, statements.length);
        assertTrue(statements[0] instanceof AddColumnStatement);
        assertTrue(statements[1] instanceof DropUniqueConstraintStatement);
        assertEquals("uniqueConstraint",
            ((DropUniqueConstraintStatement) statements[1]).getConstraintName());
        assertTrue(statements[2] instanceof RenameTableStatement);
        assertEquals("table_hst",
            ((RenameTableStatement) statements[2]).getOldTableName());
        assertEquals("table_hst_1_0_0",
            ((RenameTableStatement) statements[2]).getNewTableName());
        assertTrue(statements[3] instanceof CreateTableStatement);
        assertEquals("table_hst",
            ((CreateTableStatement) statements[3]).getTableName());
        assertTrue(statements[4] instanceof RawSqlStatement);
        assertEquals("REVOKE ALL PRIVILEGES ON TABLE table_hst FROM PUBLIC;",
            ((RawSqlStatement) statements[4]).getSql());
        assertTrue(statements[5] instanceof RawSqlStatement);
        assertEquals("CALL p_init_new_hist_table('table_hst_1_0_0', 'table_hst');",
            ((RawSqlStatement) statements[5]).getSql());
        assertTrue(statements[6] instanceof RawSqlStatement);
        assertEquals("ALTER TABLE table_hst_1_0_0 SET SCHEMA archive;",
            ((RawSqlStatement) statements[6]).getSql());
    }
    
    private AddColumnConfig mockColumn() {
        AddColumnConfig column = new AddColumnConfig();
        column.setName("column");
        column.setType("type");
        return column;
    }

    private AddColumnConfig mockColumn(ConstraintsConfig constraintsConfig) {
        AddColumnConfig column = new AddColumnConfig();
        column.setName("column1");
        column.setType("type1");
        column.setDefaultValue("defaultValue");
        column.setConstraints(constraintsConfig);
        return column;
    }

    private MockDatabase mockDatabaseWithVersion(String version)
        throws SQLException, DatabaseException {
        MockDatabase database = new MockDatabase();
        database.setConnection(mockJdbcConnection(version));
        return database;
    }

    private JdbcConnection mockJdbcConnection(String version) throws SQLException, DatabaseException {
        JdbcConnection connection = mock(JdbcConnection.class);
        Statement statement = mock(Statement.class);
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.next()).thenReturn(true);
        if(version == null) {
            when(resultSet.next()).thenReturn(false);
        }
        when(resultSet.getString(DdmConstants.METADATA_ATTRIBUTE_VALUE)).thenReturn(version);
        when(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY)).thenReturn(statement);
        when(statement.executeQuery(any())).thenReturn(resultSet);
        return connection;
    }
}
