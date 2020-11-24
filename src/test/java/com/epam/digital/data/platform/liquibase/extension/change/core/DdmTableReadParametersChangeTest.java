/*
 * Copyright 2022 EPAM Systems.
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
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableReadParametersConfig;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.RuntimeEnvironment;
import liquibase.change.ConstraintsConfig;
import liquibase.changelog.ChangeLogIterator;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.changelog.filter.ChangeSetFilterResult;
import liquibase.changelog.visitor.ChangeSetVisitor;
import liquibase.database.Database;
import liquibase.database.core.MockDatabase;
import liquibase.parser.core.xml.XMLChangeLogSAXParser;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DdmTableReadParametersChangeTest {

    private DdmTableReadParametersChange change;
    private ChangeSet changeSet;

    @BeforeEach
    void setUp() {
        DatabaseChangeLog changeLog = new DatabaseChangeLog("path");

        ChangeLogParameters changeLogParameters = new ChangeLogParameters();
        changeLog.setChangeLogParameters(changeLogParameters);

        changeSet = new ChangeSet(changeLog);

        change = new DdmTableReadParametersChange();

        changeSet.addChange(change);
        changeLog.addChangeSet(changeSet);
    }

    @Test
    void validateColumnFetchTypeErrors() {
        change.setTable("table");
        DdmTableReadParametersConfig readParametersConfig = new DdmTableReadParametersConfig();
        readParametersConfig.setName("column");
        readParametersConfig.setFetchType("entity");
        change.setReadParameters(Collections.singletonList(readParametersConfig));

        assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    void validateColumnFetchTypeWithM2mExisting() {
        DdmCreateTableChange createTableChange = new DdmCreateTableChange();
        createTableChange.setTableName("table");
        DdmColumnConfig columnConfig = new DdmColumnConfig();
        columnConfig.setName("column");
        createTableChange.setColumns(Collections.singletonList(columnConfig));
        changeSet.addChange(createTableChange);

        DdmCreateMany2ManyChange many2ManyChange = new DdmCreateMany2ManyChange();
        many2ManyChange.setMainTableName("table");
        many2ManyChange.setReferenceTableName("ref_table");
        many2ManyChange.setReferenceKeysArray("column");
        changeSet.addChange(many2ManyChange);

        change.setTable("table");
        DdmTableReadParametersConfig readParametersConfig = new DdmTableReadParametersConfig();
        readParametersConfig.setName("column");
        readParametersConfig.setFetchType("entity");
        change.setReadParameters(Collections.singletonList(readParametersConfig));

        assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    void generateStatementsForNestedM2mRead() {
        DdmCreateTableChange createTableChange = new DdmCreateTableChange();
        createTableChange.setTableName("table");
        DdmColumnConfig columnConfig = new DdmColumnConfig();
        columnConfig.setName("column");
        createTableChange.setColumns(Collections.singletonList(columnConfig));
        changeSet.addChange(createTableChange);

        DdmCreateMany2ManyChange many2ManyChange = new DdmCreateMany2ManyChange();
        many2ManyChange.setMainTableName("table");
        many2ManyChange.setReferenceTableName("ref_table");
        many2ManyChange.setReferenceKeysArray("column");
        changeSet.addChange(many2ManyChange);

        change.setTable("table");
        DdmTableReadParametersConfig readParametersConfig = new DdmTableReadParametersConfig();
        readParametersConfig.setName("column");
        readParametersConfig.setFetchType("entity");
        change.setReadParameters(Collections.singletonList(readParametersConfig));

        SqlStatement[] actual = change.generateStatements(new MockDatabase());
        assertEquals(1, actual.length);
        assertEquals("insert into ddm_liquibase_metadata(" +
                "change_type, change_name, attribute_name, attribute_value) values " +
                "('nestedRead', 'table', 'ref_table', 'column');\n\n",
                ((RawSqlStatement) actual[0]).getSql());
    }

    @Test
    void generateStatementsForNestedO2mRead() {
        DdmCreateTableChange createTableChange = new DdmCreateTableChange();
        createTableChange.setTableName("table");
        DdmColumnConfig columnConfig = new DdmColumnConfig();
        columnConfig.setName("column");
        ConstraintsConfig constraints = new ConstraintsConfig();
        constraints.setForeignKeyName("fk");
        constraints.setReferencedTableName("ref_table");
        columnConfig.setConstraints(constraints);
        createTableChange.setColumns(Collections.singletonList(columnConfig));
        changeSet.addChange(createTableChange);

        change.setTable("table");
        DdmTableReadParametersConfig readParametersConfig = new DdmTableReadParametersConfig();
        readParametersConfig.setName("column");
        readParametersConfig.setFetchType("entity");
        change.setReadParameters(Collections.singletonList(readParametersConfig));

        SqlStatement[] actual = change.generateStatements(new MockDatabase());
        assertEquals(1, actual.length);
        assertEquals("insert into ddm_liquibase_metadata(" +
                        "change_type, change_name, attribute_name, attribute_value) values " +
                        "('nestedRead', 'table', 'ref_table', 'column');\n\n",
                ((RawSqlStatement) actual[0]).getSql());
    }

    @Test
    void generateStatementsForFetchTypeId() {
        DdmCreateTableChange createTableChange = new DdmCreateTableChange();
        createTableChange.setTableName("table");
        DdmColumnConfig columnConfig = new DdmColumnConfig();
        columnConfig.setName("column");
        createTableChange.setColumns(Collections.singletonList(columnConfig));
        changeSet.addChange(createTableChange);

        change.setTable("table");
        DdmTableReadParametersConfig readParametersConfig = new DdmTableReadParametersConfig();
        readParametersConfig.setName("column");
        readParametersConfig.setFetchType("id");
        change.setReadParameters(Collections.singletonList(readParametersConfig));

        SqlStatement[] actual = change.generateStatements(new MockDatabase());
        assertEquals(1, actual.length);
        assertEquals("delete from ddm_liquibase_metadata where" +
                        " (change_type = 'nestedRead') and (change_name = 'table')" +
                        " and (attribute_value = 'column');\n\n",
                ((RawSqlStatement) actual[0]).getSql());
    }

    @Test
    void checkLoad() throws Exception {
        XMLChangeLogSAXParser xmlParser = new XMLChangeLogSAXParser();
        DdmResourceAccessor resourceAccessor = new DdmResourceAccessor();
        DatabaseChangeLog changeLog = xmlParser.parse(DdmTest.TEST_TABLE_READ_PARAMETERS_FILE_NAME,
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

        assertEquals(1, changeSets.size());
        assertTrue(changeSets.get(0).getChanges().get(0) instanceof DdmTableReadParametersChange);
        DdmTableReadParametersChange parsedChange = (DdmTableReadParametersChange) changeSets.get(0).getChanges().get(0);
        assertEquals("table", parsedChange.getTable());
        assertEquals("column1", parsedChange.getReadParameters().get(0).getName());
        assertEquals("id", parsedChange.getReadParameters().get(0).getFetchType());
        assertEquals("column2", parsedChange.getReadParameters().get(1).getName());
        assertEquals("entity", parsedChange.getReadParameters().get(1).getFetchType());

    }

}