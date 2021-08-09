package com.epam.digital.data.platform.liquibase.extension.change.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import liquibase.Contexts;
import com.epam.digital.data.platform.liquibase.extension.DdmResourceAccessor;
import com.epam.digital.data.platform.liquibase.extension.DdmTestConstants;
import liquibase.LabelExpression;
import liquibase.RuntimeEnvironment;
import liquibase.change.Change;
import liquibase.change.ConstraintsConfig;
import liquibase.change.core.DropTableChange;
import liquibase.changelog.ChangeLogIterator;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.changelog.filter.ChangeSetFilterResult;
import liquibase.changelog.visitor.ChangeSetVisitor;
import liquibase.database.Database;
import liquibase.database.core.MockDatabase;
import liquibase.database.core.PostgresDatabase;
import liquibase.exception.ChangeLogParseException;
import liquibase.exception.LiquibaseException;
import liquibase.parser.core.xml.XMLChangeLogSAXParser;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.AddDefaultValueStatement;
import liquibase.statement.core.CreateTableStatement;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmDistributeTableStatement;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmReferenceTableStatement;
import liquibase.statement.core.DropPrimaryKeyStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.statement.core.SetColumnRemarksStatement;
import liquibase.statement.core.SetTableRemarksStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmCreateTableChangeTest {
    private DdmCreateTableChange change;

    @BeforeEach
    void setUp() {
        change = new DdmCreateTableChange();
        change.setTableName("table");

        DdmColumnConfig column1 = new DdmColumnConfig();
        column1.setName("column1");
        column1.setType("type1");
        ConstraintsConfig constraint = new ConstraintsConfig();
        constraint.setPrimaryKey("true");
        column1.setConstraints(constraint);
        change.addColumn(column1);

        DdmColumnConfig column2 = new DdmColumnConfig();
        column2.setName("column2");
        column2.setType("type2");
        change.addColumn(column2);
    }

    @Test
    @DisplayName("Validate change")
    public void validateChange() {
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - local")
    public void validateChangeLocal() {
        change.setDistribution("local");
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - distributePrimary")
    public void validateChangeDistributePrimary() {
        change.setDistribution("distributePrimary");
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - distributeAll")
    public void validateChangeDistributeAll() {
        change.setDistribution("distributeAll");
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - distributeHistory")
    public void validateChangeDistributeHistory() {
        change.setDistribution("distributeHistory");
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - referencePrimary")
    public void validateChangeReferencePrimary() {
        change.setDistribution("referencePrimary");
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - referenceAll")
    public void validateChangeReferenceAll() {
        change.setDistribution("referenceAll");
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - referenceHistory")
    public void validateChangeReferenceHistory() {
        change.setDistribution("referenceHistory");
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Check statements")
    public void checkStatements() {
        change.setHistoryFlag(true);

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(7, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[3] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[4] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[5] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[6] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check statements - isObject")
    public void checkStatementsIsObject() {
        change.setHistoryFlag(true);
        change.setIsObject(true);

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(7, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[3] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[4] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[5] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[6] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check statements - local")
    public void checkStatementsLocal() {
        change.setHistoryFlag(true);
        change.setDistribution("local");

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(7, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[3] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[4] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[5] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[6] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check statements - distributeAll")
    public void checkStatementsDistributeAll() {
        change.setHistoryFlag(true);
        change.setDistribution("distributeAll");

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(9, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof DdmDistributeTableStatement);
        Assertions.assertTrue(statements[3] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[4] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[5] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[6] instanceof DdmDistributeTableStatement);
        Assertions.assertTrue(statements[7] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[8] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check statements - distributePrimary")
    public void checkStatementsDistributePrimary() {
        change.setHistoryFlag(true);
        change.setDistribution("distributePrimary");

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(8, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[3] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[4] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[5] instanceof DdmDistributeTableStatement);
        Assertions.assertTrue(statements[6] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[7] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check statements - distributeHistory")
    public void checkStatementsDistributeHistory() {
        change.setHistoryFlag(true);
        change.setDistribution("distributeHistory");

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(8, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof DdmDistributeTableStatement);
        Assertions.assertTrue(statements[3] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[4] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[5] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[6] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[7] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check statements - referenceAll")
    public void checkStatementsReferenceAll() {
        change.setHistoryFlag(true);
        change.setDistribution("referenceAll");

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(9, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof DdmReferenceTableStatement);
        Assertions.assertTrue(statements[3] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[4] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[5] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[6] instanceof DdmReferenceTableStatement);
        Assertions.assertTrue(statements[7] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[8] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check statements - referencePrimary")
    public void checkStatementsReferencePrimary() {
        change.setHistoryFlag(true);
        change.setDistribution("referencePrimary");

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(8, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[3] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[4] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[5] instanceof DdmReferenceTableStatement);
        Assertions.assertTrue(statements[6] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[7] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check statements - referenceHistory")
    public void checkStatementsReferenceHistory() {
        change.setHistoryFlag(true);
        change.setDistribution("referenceHistory");

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(8, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof DdmReferenceTableStatement);
        Assertions.assertTrue(statements[3] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[4] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[5] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[6] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[7] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check statements - table remarks")
    public void checkStatementsRemarks() {
        change.setHistoryFlag(true);
        change.setRemarks("remark");

        SqlStatement[] statements = change.generateStatements(new PostgresDatabase());
        Assertions.assertEquals(9, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof SetTableRemarksStatement);
        Assertions.assertTrue(statements[3] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[4] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[5] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[6] instanceof SetTableRemarksStatement);
        Assertions.assertTrue(statements[7] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[8] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check statements - column remarks")
    public void checkStatementsColumnRemarks() {
        change.setHistoryFlag(true);
        change.getColumns().get(0).setRemarks("remark");

        SqlStatement[] statements = change.generateStatements(new PostgresDatabase());
        Assertions.assertEquals(9, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof SetColumnRemarksStatement);
        Assertions.assertTrue(statements[3] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[4] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[5] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[6] instanceof SetColumnRemarksStatement);
        Assertions.assertTrue(statements[7] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[8] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check classify")
    public void checkClassify() {
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column3");
        column.setType("type3");
        column.setClassify("private");
        change.addColumn(column);

        change.setHistoryFlag(true);

        SqlStatement[] statements = change.generateStatements(new PostgresDatabase());
        Assertions.assertEquals(8, statements.length);
        Assertions.assertTrue(statements[0] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[1] instanceof DropPrimaryKeyStatement);
        Assertions.assertTrue(statements[2] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[3] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[4] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[5] instanceof AddDefaultValueStatement);
        Assertions.assertTrue(statements[6] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[7] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check inverse")
    public void checkInverse() {
        Change[] changes = change.createInverses();
        Assertions.assertEquals(1, changes.length);
        Assertions.assertTrue(changes[0] instanceof DropTableChange);
    }

    @Test
    @DisplayName("Check inverse - history")
    public void checkInverseHistory() {
        change.setHistoryFlag(true);

        Change[] changes = change.createInverses();
        Assertions.assertEquals(2, changes.length);
        Assertions.assertTrue(changes[0] instanceof DropTableChange);
        Assertions.assertTrue(changes[1] instanceof DropTableChange);
    }

    @Test
    @DisplayName("Check load")
    public void checkLoad() throws ChangeLogParseException, Exception {
        XMLChangeLogSAXParser xmlParser = new XMLChangeLogSAXParser();
        DdmResourceAccessor resourceAccessor = new DdmResourceAccessor();
        DatabaseChangeLog changeLog = xmlParser.parse(DdmTestConstants.TEST_CREATE_TABLE_FILE_NAME,
            new ChangeLogParameters(), resourceAccessor);

        final List<ChangeSet> changeSets = new ArrayList<ChangeSet>();

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