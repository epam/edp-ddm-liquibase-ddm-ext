package com.epam.digital.data.platform.liquibase.extension.change.core;

import static com.epam.digital.data.platform.liquibase.extension.DdmConstants.CONTEXT_PUB;
import static com.epam.digital.data.platform.liquibase.extension.DdmConstants.CONTEXT_SUB;

import com.epam.digital.data.platform.liquibase.extension.DdmTest;
import com.epam.digital.data.platform.liquibase.extension.change.DdmRoleConfig;
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

class DdmGrantAllChangeTest {
    private DdmGrantAllChange change;
    private ChangeLogParameters changeLogParameters;
    private ChangeSet changeSet;

    @BeforeEach
    void setUp() {
        change = new DdmGrantAllChange();
        DatabaseChangeLog changeLog = new DatabaseChangeLog("path");
        changeSet = new ChangeSet(changeLog);
        changeSet.addChange(change);
        change.setChangeSet(changeSet);

        changeLogParameters = new ChangeLogParameters();
        changeLog.setChangeLogParameters(changeLogParameters);
    }

    void setRoles(String roleName) {
        List<DdmRoleConfig> roles = new ArrayList<>();

        DdmRoleConfig role = new DdmRoleConfig();
        role.setName(roleName);

        roles.add(role);
        change.setRoles(roles);
    }

    @Test
    @DisplayName("Validate change")
    public void validateChange() {
        setContext(CONTEXT_SUB);
        setRoles("role");
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());

        setRoles("");
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());

        setRoles(" ");
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Check statements")
    public void checkStatements() {
        setContext(CONTEXT_SUB);
        setRoles("role");
        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(1, statements.length);
        Assertions.assertTrue(statements[0] instanceof RawSqlStatement);
        Assertions.assertEquals("CALL p_grant_analytics_user ('role');", ((RawSqlStatement) statements[0]).getSql());
    }

    @Test
    @DisplayName("Check confirmation message")
    public void checkConfirmationMessage() {
        Assertions.assertEquals("Permissions to all report views have been set", change.getConfirmationMessage());
    }

    @Test
    @DisplayName("Validate inverse")
    public void validateInverse() {
        setContext(CONTEXT_SUB);
        setRoles("role");
        Change[] changes = change.createInverses();
        changes[0].setChangeSet(change.getChangeSet());
        Assertions.assertEquals(0, changes[0].validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Check load")
    public void checkLoad() throws Exception {
        Assertions.assertEquals(1, DdmTest.loadChangeSets(DdmTest.TEST_GRANT_ALL_FILE_NAME).size());
    }

    @Test
    public void shouldIgnoreChangeSetForContextPub() {
        setContext(CONTEXT_PUB);
        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(0, statements.length);
        Assertions.assertTrue(change.getChangeSet().isIgnore());
    }
    private void setContext(String ctx) {
        Contexts contexts = new Contexts();
        contexts.add(ctx);
        changeLogParameters.setContexts(contexts);
    }
}