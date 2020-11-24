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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.epam.digital.data.platform.liquibase.extension.DdmMockSnapshotGeneratorFactory;
import com.epam.digital.data.platform.liquibase.extension.DdmResourceAccessor;
import com.epam.digital.data.platform.liquibase.extension.DdmTest;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.RuntimeEnvironment;
import liquibase.changelog.ChangeLogIterator;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.changelog.filter.ChangeSetFilterResult;
import liquibase.changelog.visitor.ChangeSetVisitor;
import liquibase.database.Database;
import liquibase.database.core.MockDatabase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.parser.core.xml.XMLChangeLogSAXParser;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmMakeObjectChangeTest {
    private DdmMakeObjectChange change;
    private DdmMakeObjectChange snapshotChange;

    @BeforeEach
    void setUp() {
        DatabaseChangeLog changeLog = new DatabaseChangeLog("path");

        ChangeLogParameters changeLogParameters = new ChangeLogParameters();
        changeLog.setChangeLogParameters(changeLogParameters);

        ChangeSet changeSet = new ChangeSet(changeLog);

        change = new DdmMakeObjectChange();
        change.setTables(createTables());
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

        snapshotChange = new DdmMakeObjectChange(new DdmMockSnapshotGeneratorFactory(historyTable));
        snapshotChange.setTables(createTables());
        snapshotChange.setChangeSet(changeSet);
    }

    @Test
    @DisplayName("Check load makeObject")
    void checkLoad() throws Exception {
        XMLChangeLogSAXParser xmlParser = new XMLChangeLogSAXParser();
        DdmResourceAccessor resourceAccessor = new DdmResourceAccessor();
        DatabaseChangeLog changeLog = xmlParser.parse(DdmTest.TEST_MAKE_OBJECT_FILE_NAME,
            new ChangeLogParameters(), resourceAccessor);

        final List<ChangeSet> changeSets = new ArrayList<>();

        new ChangeLogIterator(changeLog).run(new ChangeSetVisitor() {
            @Override
            public Direction getDirection() {
                return Direction.FORWARD;
            }

            @Override
            public void visit(ChangeSet changeSet, DatabaseChangeLog databaseChangeLog, Database database, Set<ChangeSetFilterResult> filterResults) {
                changeSets.add(changeSet);
            }
        }, new RuntimeEnvironment(new MockDatabase(), new Contexts(), new LabelExpression()));

        Assertions.assertEquals(1, changeSets.size());
    }

    @Test
    @DisplayName("Validate change")
    void validateChange() {
        Assertions.assertEquals(0, snapshotChange.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Check statements")
    void checkStatements() throws SQLException, DatabaseException {
        MockDatabase database = new MockDatabase();
        database.setConnection(mockJdbcConnection("1.0.0"));
        
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
    @DisplayName("Check statements with null version")
    void checkStatementsWithNullVersion() throws SQLException, DatabaseException {
        MockDatabase database = new MockDatabase();
        database.setConnection(mockJdbcConnection(null));
        
        SqlStatement[] statements = snapshotChange.generateStatements(database);
        
        Assertions.assertEquals(2, statements.length);
        Assertions.assertTrue(statements[0] instanceof AddColumnStatement);
        Assertions.assertTrue(statements[1] instanceof AddColumnStatement);
        Assertions.assertEquals("table", ((AddColumnStatement) statements[0]).getTableName());
        Assertions.assertEquals("table_hst", ((AddColumnStatement) statements[1]).getTableName());
    }

    @Test
    @DisplayName("Check messages")
    void checkMessages() {
        Assertions.assertEquals("Objects have been made", snapshotChange.getConfirmationMessage());
        Assertions.assertEquals("http://www.liquibase.org/xml/ns/dbchangelog", snapshotChange.getSerializedObjectNamespace());
    }

    private List<DdmTableConfig> createTables() {
        List<DdmTableConfig> tables = new ArrayList<>();

        DdmTableConfig table = new DdmTableConfig();
        table.setName("table");

        tables.add(table);

        return tables;
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