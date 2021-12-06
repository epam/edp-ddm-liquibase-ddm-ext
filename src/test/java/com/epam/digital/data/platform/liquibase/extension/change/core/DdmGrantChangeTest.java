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

import static com.epam.digital.data.platform.liquibase.extension.DdmConstants.CONTEXT_SUB;

import com.epam.digital.data.platform.liquibase.extension.DdmTest;
import com.epam.digital.data.platform.liquibase.extension.change.DdmRoleConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import java.util.ArrayList;
import java.util.List;
import liquibase.Contexts;
import liquibase.change.Change;
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

class DdmGrantChangeTest {
    private DdmGrantChange change;
    private ChangeLogParameters changeLogParameters;
    private ChangeSet changeSet;

    @BeforeEach
    void setUp() {
        change = new DdmGrantChange();
        DatabaseChangeLog changeLog = new DatabaseChangeLog("path");
        changeSet = new ChangeSet(changeLog);
        changeSet.addChange(change);
        change.setChangeSet(changeSet);

        changeLogParameters = new ChangeLogParameters();
        changeLog.setChangeLogParameters(changeLogParameters);
    }

    void setRoles(String roleName, String viewName) {
        List<DdmRoleConfig> roles = new ArrayList<>();
        List<DdmTableConfig> views = new ArrayList<>();

        DdmTableConfig view = new DdmTableConfig();
        view.setName(viewName);
        views.add(view);

        DdmRoleConfig role = new DdmRoleConfig();
        role.setName(roleName);
        role.setTables(views);

        roles.add(role);
        change.setRoles(roles);
    }

    @Test
    @DisplayName("Validate change")
    public void validateChange() {
        setContext(CONTEXT_SUB);
        setRoles("role", "view");
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());

        setRoles("", "view");
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());

        setRoles(" ", "view");
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());

        setRoles("role", "");
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());

        setRoles("role", " ");
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Check statements")
    public void checkStatements() {
        setContext(CONTEXT_SUB);
        setRoles("role", "view");
        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(1, statements.length);
        Assertions.assertTrue(statements[0] instanceof RawSqlStatement);
        Assertions.assertEquals("CALL p_grant_analytics_user ('role','view_v');", ((RawSqlStatement) statements[0]).getSql());
    }

    @Test
    @DisplayName("Check confirmation message")
    public void checkConfirmationMessage() {
        Assertions.assertEquals("Permissions have been set", change.getConfirmationMessage());
    }

    @Test
    @DisplayName("Validate inverse")
    public void validateInverse() {
        setContext(CONTEXT_SUB);
        setRoles("role", "view");
        Change[] changes = change.createInverses();
        changes[0].setChangeSet(change.getChangeSet());
        Assertions.assertEquals(0, changes[0].validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Check load")
    public void checkLoad() throws Exception {
        Assertions.assertEquals(1, DdmTest.loadChangeSets(DdmTest.TEST_GRANT_FILE_NAME).size());
    }

    private void setContext(String ctx) {
        Contexts contexts = new Contexts();
        contexts.add(ctx);
        changeLogParameters.setContexts(contexts);
    }
}