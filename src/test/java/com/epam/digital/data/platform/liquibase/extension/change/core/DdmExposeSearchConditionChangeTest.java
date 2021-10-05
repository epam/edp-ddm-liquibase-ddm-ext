package com.epam.digital.data.platform.liquibase.extension.change.core;

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

    @BeforeEach
    void setUp() {
        change = new DdmExposeSearchConditionChange();
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

        changeLog.addChangeSet(changeSet);

        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
        Assertions.assertEquals("Expose Search Condition name", change.getConfirmationMessage());
        Assertions.assertEquals("http://www.liquibase.org/xml/ns/dbchangelog", change.getSerializedObjectNamespace());
    }
}