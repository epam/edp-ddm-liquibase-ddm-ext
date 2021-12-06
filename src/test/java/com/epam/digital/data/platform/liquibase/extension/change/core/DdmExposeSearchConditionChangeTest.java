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

import liquibase.Contexts;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.core.MockDatabase;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmExposeSearchConditionChangeTest {
    private DdmExposeSearchConditionChange change;
    private ChangeLogParameters changeLogParameters;
    private ChangeSet changeSet;

    @BeforeEach
    void setUp() {
        change = new DdmExposeSearchConditionChange();
        DatabaseChangeLog changeLog = new DatabaseChangeLog("path");
        changeSet = new ChangeSet(changeLog);
        change.setChangeSet(changeSet);

        changeLogParameters = new ChangeLogParameters();
        changeLog.setChangeLogParameters(changeLogParameters);
    }

    @Test
    @DisplayName("Check statements")
    public void checkStatements() {
        change.setName("name");
        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(1, statements.length);
        Assertions.assertTrue(statements[0] instanceof RawSqlStatement);
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
    @DisplayName("Validate change")
    public void validateChange() {
        DatabaseChangeLog changeLog = new DatabaseChangeLog("path");
        ChangeSet changeSet = new ChangeSet(changeLog);

        change.setName("name");
        change.setConsumer("consumer");

        changeSet.addChange(change);

        DdmCreateSearchConditionChange scChange = new DdmCreateSearchConditionChange();
        scChange.setName("name");
        changeSet.addChange(scChange);
        changeLog.setChangeLogParameters(changeLogParameters);

        changeLog.addChangeSet(changeSet);

        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
        Assertions.assertEquals("Expose Search Condition name", change.getConfirmationMessage());
        Assertions.assertEquals("http://www.liquibase.org/xml/ns/dbchangelog", change.getSerializedObjectNamespace());
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