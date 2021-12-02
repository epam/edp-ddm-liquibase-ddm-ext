package com.epam.digital.data.platform.liquibase.extension.change.core;

import liquibase.Contexts;
import liquibase.change.AddColumnConfig;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.core.MockDatabase;
import liquibase.statement.SqlStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;

class DdmCreateAnalyticsIndexChangeTest {
    private DdmCreateAnalyticsIndexChange change;
    private ChangeLogParameters changeLogParameters;
    private ChangeSet changeSet;

    @BeforeEach
    void setUp() {
        change = new DdmCreateAnalyticsIndexChange();
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
    @DisplayName("Validate change - only analytics tags allowed")
    public void validateAllowedTags() {
        AddColumnConfig config = new AddColumnConfig();
        config.setName("column");
        change.setColumns(Collections.singletonList(config));
        change.setTableName("table");
        DdmCreateSearchConditionChange scChange = new DdmCreateSearchConditionChange();
        scChange.setName("name");
        changeSet.addChange(scChange);
        changeSet.addChange(change);
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }
}