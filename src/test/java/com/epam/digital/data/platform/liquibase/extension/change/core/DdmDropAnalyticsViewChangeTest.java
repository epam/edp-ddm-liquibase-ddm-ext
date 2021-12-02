package com.epam.digital.data.platform.liquibase.extension.change.core;

import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmDropAnalyticsViewStatement;
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

class DdmDropAnalyticsViewChangeTest {
    private DdmDropAnalyticsViewChange change;
    private ChangeLogParameters changeLogParameters;
    private ChangeSet changeSet;

    @BeforeEach
    void setUp() {
        change = new DdmDropAnalyticsViewChange();
        DatabaseChangeLog changeLog = new DatabaseChangeLog("path");
        changeSet = new ChangeSet(changeLog);
        change.setChangeSet(changeSet);

        changeLogParameters = new ChangeLogParameters();
        changeLog.setChangeLogParameters(changeLogParameters);
    }

    @Test
    @DisplayName("Check ignore")
    public void checkIgnoreChangeSetForContextPub() {
        Contexts contexts = new Contexts();
        contexts.add("pub");
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
        Assertions.assertTrue(statements[0] instanceof DdmDropAnalyticsViewStatement);
    }

    @Test
    @DisplayName("Validate change")
    public void validateChange() {
        change.setName("name");
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
        Assertions.assertEquals("Analytics View name dropped", change.getConfirmationMessage());
        Assertions.assertEquals("http://www.liquibase.org/xml/ns/dbchangelog", change.getSerializedObjectNamespace());
    }

    @Test
    @DisplayName("Validate change - name is required")
    public void validateChangeName() {
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - only analytics tags allowed")
    public void validateAllowedTags() {
        change.setName("name");
        DdmCreateSearchConditionChange scChange = new DdmCreateSearchConditionChange();
        scChange.setName("name");
        changeSet.addChange(scChange);
        changeSet.addChange(change);
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }
}