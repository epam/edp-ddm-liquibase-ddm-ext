package com.epam.digital.data.platform.liquibase.extension.change.core;

import com.epam.digital.data.platform.liquibase.extension.DdmResourceAccessor;
import com.epam.digital.data.platform.liquibase.extension.DdmTestConstants;
import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmRoleConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import liquibase.*;
import liquibase.changelog.ChangeLogIterator;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.changelog.filter.ChangeSetFilterResult;
import liquibase.changelog.visitor.ChangeSetVisitor;
import liquibase.database.Database;
import liquibase.database.core.MockDatabase;
import liquibase.exception.ChangeLogParseException;
import liquibase.exception.LiquibaseException;
import liquibase.parser.core.xml.XMLChangeLogSAXParser;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.DeleteStatement;
import liquibase.statement.core.RawSqlStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

class DdmRbacChangeTest {
    private DdmRbacChange change;

    @BeforeEach
    void setUp() {
        change = new DdmRbacChange();
    }

    @Test
    @DisplayName("Validate")
    public void validate() {
        DdmColumnConfig column;
        DdmTableConfig table = new DdmTableConfig("table");
        DdmRoleConfig role = new DdmRoleConfig();
        role.setName("name");

        column = new DdmColumnConfig();
        column.setName("column");
        column.setRead(true);
        column.setUpdate(false);
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column2");
        column.setRead(false);
        column.setUpdate(true);
        table.addColumn(column);

        table.setRoleCanDelete(true);
        table.setRoleCanInsert(true);

        role.addTable(table);
        change.addRole(role);

        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(5, statements.length);
        Assertions.assertTrue(statements[0] instanceof DeleteStatement);
        Assertions.assertTrue(statements[1] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[2] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[3] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[4] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Validate - table has read and update access")
    public void validateTable() {
        DdmColumnConfig column;
        DdmTableConfig table = new DdmTableConfig("table");
        DdmRoleConfig role = new DdmRoleConfig();
        role.setName("name");

        table.setRoleCanRead(true);
        table.setRoleCanUpdate(true);

        role.addTable(table);
        change.addRole(role);

        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(3, statements.length);
        Assertions.assertTrue(statements[0] instanceof DeleteStatement);
        Assertions.assertTrue(statements[1] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[2] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Validate - table and columns have the same access")
    public void validateTablePlusColumn() {
        DdmColumnConfig column;
        DdmTableConfig table = new DdmTableConfig("table");
        DdmRoleConfig role = new DdmRoleConfig();
        role.setName("name");

        column = new DdmColumnConfig();
        column.setName("column");
        column.setRead(true);
        column.setUpdate(false);
        table.addColumn(column);

        table.setRoleCanRead(true);
        table.setRoleCanUpdate(true);

        role.addTable(table);
        change.addRole(role);

        role = new DdmRoleConfig();
        role.setName("name");
        table = new DdmTableConfig("table");
        role.addTable(table);
        change.addRole(role);

        Assertions.assertEquals(3, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Check load rbac")
    public void checkLoad() throws ChangeLogParseException, Exception {
        XMLChangeLogSAXParser xmlParser = new XMLChangeLogSAXParser();
        DdmResourceAccessor resourceAccessor = new DdmResourceAccessor();
        DatabaseChangeLog changeLog = xmlParser.parse(DdmTestConstants.TEST_RBAC_FILE_NAME,
                new ChangeLogParameters(), resourceAccessor);

        final List<ChangeSet> changeSets = new ArrayList<>();

        new ChangeLogIterator(changeLog).run(new ChangeSetVisitor() {
            @Override
            public Direction getDirection() {
                return Direction.FORWARD;
            }

            @Override
            public void visit(ChangeSet changeSet, DatabaseChangeLog databaseChangeLog, Database database, Set<ChangeSetFilterResult> filterResults) throws LiquibaseException {
                changeSets.add(changeSet);
            }
        }, new RuntimeEnvironment(new MockDatabase(), new Contexts(), new LabelExpression()));

        Assertions.assertEquals(1, changeSets.size());
    }
}