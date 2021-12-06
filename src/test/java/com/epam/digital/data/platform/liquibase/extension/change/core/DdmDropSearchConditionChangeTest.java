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

import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmDropSearchConditionStatement;
import liquibase.Contexts;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.core.MockDatabase;
import liquibase.statement.SqlStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmDropSearchConditionChangeTest {
    private DdmDropSearchConditionChange change;
    private ChangeLogParameters changeLogParameters;
    private ChangeSet changeSet;

    @BeforeEach
    void setUp() {
        change = new DdmDropSearchConditionChange();
        DatabaseChangeLog changeLog = new DatabaseChangeLog("path");
        changeSet = new ChangeSet(changeLog);
        change.setChangeSet(changeSet);

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
    @DisplayName("Check statements")
    public void checkStatements() {
        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(1, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmDropSearchConditionStatement);
    }

    @Test
    @DisplayName("Validate change")
    public void validateChange() {
        change.setName("name");
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
        Assertions.assertEquals("Search Condition name dropped", change.getConfirmationMessage());
        Assertions.assertEquals("http://www.liquibase.org/xml/ns/dbchangelog", change.getSerializedObjectNamespace());
    }

    @Test
    @DisplayName("Validate change - name is required")
    public void validateChangeName() {
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