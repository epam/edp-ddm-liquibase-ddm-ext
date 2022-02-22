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
import liquibase.change.ConstraintsConfig;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.core.MockDatabase;
import liquibase.statement.SqlStatement;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateMany2ManyStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmCreateMany2ManyChangeTest {
    private DdmCreateMany2ManyChange change;

    @BeforeEach
    void setUp() {
        DatabaseChangeLog changeLog = new DatabaseChangeLog("path");
        ChangeSet changeSet = new ChangeSet(changeLog);

        DdmCreateTableChange tableChange = new DdmCreateTableChange();
        tableChange.setTableName("referenceTable");
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("ref_id");
        column.setType("UUID");
        ConstraintsConfig constraint = new ConstraintsConfig();
        constraint.setPrimaryKeyName("pk_ref");
        column.setConstraints(constraint);
        tableChange.addColumn(column);

        changeSet.addChange(tableChange);
        change = new DdmCreateMany2ManyChange();

        changeSet.addChange(change);
        changeLog.addChangeSet(changeSet);
    }

    @Test
    @DisplayName("Check statements")
    public void checkStatements() {
        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(1, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmCreateMany2ManyStatement);
    }

    @Test
    @DisplayName("Validate change")
    public void validateChange() {
        change.setMainTableName("mainTable");
        change.setMainTableKeyField("keyField");
        change.setReferenceTableName("referenceTable");
        change.setReferenceKeysArray("keysArray");
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - reference table is required")
    public void validateChangeNoReferenceTable() {
        DatabaseChangeLog changeLog = new DatabaseChangeLog("path");
        ChangeSet changeSet = new ChangeSet(changeLog);

        change.setMainTableName("mainTable");
        change.setMainTableKeyField("keyField");
        change.setReferenceTableName("referenceTable");
        change.setReferenceKeysArray("keysArray");

        DdmCreateTableChange tableChange = new DdmCreateTableChange();
        tableChange.setTableName("another");

        changeSet.addChange(change);
        changeSet.addChange(tableChange);
        changeLog.addChangeSet(changeSet);

        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
        Assertions.assertEquals("Table referencetable or corresponding primary key column doesn't exist",
            change.validate(new MockDatabase()).getErrorMessages().get(0));
    }

    @Test
    @DisplayName("Validate change - mainTableName is required")
    public void validateChangeMainTable() {
        change.setMainTableKeyField("keyField");
        change.setReferenceTableName("referenceTable");
        change.setReferenceKeysArray("keysArray");
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - mainTableKeyField is required")
    public void validateChangeKeyField() {
        change.setMainTableName("mainTable");
        change.setReferenceTableName("referenceTable");
        change.setReferenceKeysArray("keysArray");
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - referenceTableName is required")
    public void validateChangeReferenceTable() {
        change.setMainTableName("mainTable");
        change.setMainTableKeyField("keyField");
        change.setReferenceKeysArray("keysArray");
        Assertions.assertEquals(2, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - referenceKeysArray is required")
    public void validateChangeKeysArray() {
        change.setMainTableName("mainTable");
        change.setMainTableKeyField("keyField");
        change.setReferenceTableName("referenceTable");
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Check load")
    public void checkLoad() throws Exception {
        Assertions.assertEquals(1, DdmTest.loadChangeSets(DdmTest.TEST_CREATE_M2M_FILE_NAME).size());
    }

}