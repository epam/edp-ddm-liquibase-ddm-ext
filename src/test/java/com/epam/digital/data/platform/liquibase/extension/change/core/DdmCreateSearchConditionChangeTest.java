package com.epam.digital.data.platform.liquibase.extension.change.core;

import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmFunctionConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmJoinConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import liquibase.Contexts;
import com.epam.digital.data.platform.liquibase.extension.DdmResourceAccessor;
import com.epam.digital.data.platform.liquibase.extension.DdmTestConstants;
import liquibase.LabelExpression;
import liquibase.RuntimeEnvironment;
import liquibase.change.Change;
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
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateSearchConditionStatement;
import liquibase.statement.core.InsertStatement;
import liquibase.statement.core.RawSqlStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

class DdmCreateSearchConditionChangeTest {
    private DdmCreateSearchConditionChange change;
    private ChangeLogParameters changeLogParameters;

    @BeforeEach
    void setUp() {
        DatabaseChangeLog changeLog = new DatabaseChangeLog("path");

        changeLogParameters = new ChangeLogParameters();
        changeLog.setChangeLogParameters(changeLogParameters);

        ChangeSet changeSet = new ChangeSet(changeLog);

        change = new DdmCreateSearchConditionChange();

        changeSet.addChange(change);
        changeLog.addChangeSet(changeSet);
    }

    @Test
    @DisplayName("Check statements")
    public void checkStatements() {
        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(2, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmCreateSearchConditionStatement);
        Assertions.assertTrue(statements[1] instanceof RawSqlStatement);  //  grant select to view
    }

    @Test
    @DisplayName("Check statements - insert")
    public void checkStatementsInsert() {
        DdmTableConfig table = new DdmTableConfig();
        table.setName("table");
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        column.setReturning(true);
        column.setSearchType("equal");
        table.addColumn(column);
        change.addTable(table);

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(5, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmCreateSearchConditionStatement);
        Assertions.assertTrue(statements[1] instanceof RawSqlStatement);  //  grant select to view
        Assertions.assertTrue(statements[2] instanceof InsertStatement);  //  column or alias
        Assertions.assertTrue(statements[3] instanceof InsertStatement);  //  mapping column
        Assertions.assertTrue(statements[4] instanceof InsertStatement);  //  searchType
    }

    @Test
    @DisplayName("Check statements - limit")
    public void checkStatementsLimit() {
        DdmTableConfig table = new DdmTableConfig("table");
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        column.setSearchType("contains");
        column.setReturning(true);
        table.addColumn(column);
        change.addTable(table);
        change.setLimit("20");

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(6, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmCreateSearchConditionStatement);
        Assertions.assertTrue(statements[1] instanceof RawSqlStatement);  //  grant select to view
        Assertions.assertTrue(statements[2] instanceof InsertStatement);  //  column or alias
        Assertions.assertTrue(statements[3] instanceof InsertStatement);  //  mapping column
        Assertions.assertTrue(statements[4] instanceof InsertStatement);  //  searchType
        Assertions.assertTrue(statements[5] instanceof InsertStatement);  //  limit
    }

    @Test
    @DisplayName("Validate change")
    public void validateChange() {
        change.setName("name");
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - name is required")
    public void validateChangeName() {
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - join amount of columns")
    public void validateChangeJoins() {
        change.setName("name");
        DdmJoinConfig join = new DdmJoinConfig();
        join.addLeftColumn("leftColumn");
        join.addRightColumn("rightColumn");
        change.addJoin(join);
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - lists")
    public void validateChangeLists() {
        change.setName("name");
        change.setConditions(new ArrayList<>());
        change.setTables(new ArrayList<>());
        change.setJoins(new ArrayList<>());
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - join amount of left and right columns must be equal")
    public void validateChangeJoinsEqual() {
        change.setName("name");
        DdmJoinConfig join = new DdmJoinConfig();
        join.addLeftColumn("leftColumn");
        change.addJoin(join);
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - search column and type are defined")
    public void validateChangeIndexing() {
        change.setName("name");
        change.setIndexing(true);
        DdmTableConfig table = new DdmTableConfig("table");
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        column.setSearchType("startsWith");
        table.addColumn(column);
        change.addTable(table);
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - search column is not defined")
    public void validateChangeIndexingColumn() {
        DatabaseChangeLog changeLog1 = new DatabaseChangeLog("path");

        ChangeLogParameters changeLogParameters1 = new ChangeLogParameters();
        changeLog1.setChangeLogParameters(changeLogParameters1);

        ChangeSet changeSet1 = new ChangeSet(changeLog1);

        DdmCreateSearchConditionChange change1 = new DdmCreateSearchConditionChange("name");

        changeSet1.addChange(change1);
        changeLog1.addChangeSet(changeSet1);

        change1.setLimit("all");
        change1.setIndexing(true);
        Assertions.assertEquals(1, change1.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - search column type is not defined")
    public void validateChangeIndexingColumnType() {
        change.setName("name");
        change.setIndexing(true);
        DdmTableConfig table = new DdmTableConfig("table");
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        table.addColumn(column);
        change.addTable(table);
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Confirmation Message")
    public void confirmationMessage() {
        change.setName("name");

        Assertions.assertEquals("Search Condition name created", change.getConfirmationMessage());
    }

    @Test
    @DisplayName("Validate change - pagination")
    public void validateChangePagination() {
        change.setName("name");
        change.setPagination(true);
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Check statements - pagination")
    public void checkStatementsPagination() {
        DdmTableConfig table = new DdmTableConfig("table");
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        column.setReturning(true);
        table.addColumn(column);
        change.addTable(table);
        change.setPagination(true);

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(5, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmCreateSearchConditionStatement);
        Assertions.assertTrue(statements[1] instanceof RawSqlStatement);  //  grant select to view
        Assertions.assertTrue(statements[2] instanceof InsertStatement);  //  column or alias
        Assertions.assertTrue(statements[3] instanceof InsertStatement);  //  mapping column
        Assertions.assertTrue(statements[4] instanceof InsertStatement);  //  pagination
    }

    @Test
    @DisplayName("Check statements - parameters SUB")
    public void checkStatementsSub() {
        Contexts contexts = new Contexts();
        contexts.add("sub");
        changeLogParameters.setContexts(contexts);

        DdmTableConfig table = new DdmTableConfig("table");
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        table.addColumn(column);
        change.addTable(table);

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(4, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmCreateSearchConditionStatement);
        Assertions.assertTrue(statements[1] instanceof RawSqlStatement);  //  grant select to view
        Assertions.assertTrue(statements[2] instanceof RawSqlStatement);  //  grant select to view
        Assertions.assertTrue(statements[3] instanceof InsertStatement);  //  column or alias
    }

    @Test
    @DisplayName("Check load")
    public void checkLoad() throws ChangeLogParseException, Exception {
        XMLChangeLogSAXParser xmlParser = new XMLChangeLogSAXParser();
        DdmResourceAccessor resourceAccessor = new DdmResourceAccessor();
        DatabaseChangeLog changeLog = xmlParser.parse(DdmTestConstants.TEST_CREATE_SEARCH_CONDITION_FILE_NAME,
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

    @Test
    @DisplayName("Validate inverse")
    public void validateInverse() {
        change.setName("name");
        Change[] changes = change.createInverses();
        Assertions.assertEquals(0, changes[0].validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate SQL - functions: parameter is required")
    public void validateSQLFunctionsParameterRequired() {
        change.setName("name");
        DdmColumnConfig column;
        DdmTableConfig table;

        table = new DdmTableConfig("table");
        table.setAlias("t");

        column = new DdmColumnConfig();
        column.setName("column");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column2");
        table.addColumn(column);

        List<DdmFunctionConfig> functions = new ArrayList<>();
        DdmFunctionConfig function = new DdmFunctionConfig();
        function.setTableAlias("t");
        function.setColumnName("column");
        function.setName("string_agg");
        function.setAlias("aggregated");
        functions.add(function);

        table.setFunctions(functions);
        change.addTable(table);

        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate SQL - functions: parameter is extra")
    public void validateSQLFunctionsParameterNotRequired() {
        change.setName("name");
        DdmColumnConfig column;
        DdmTableConfig table;

        table = new DdmTableConfig("table");
        table.setAlias("t");

        column = new DdmColumnConfig();
        column.setName("column");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column2");
        table.addColumn(column);

        List<DdmFunctionConfig> functions = new ArrayList<>();
        DdmFunctionConfig function = new DdmFunctionConfig();
        function.setTableAlias("t");
        function.setColumnName("column");
        function.setName("count");
        function.setAlias("cnt");
        function.setParameter("','");
        functions.add(function);

        table.setFunctions(functions);
        change.addTable(table);

        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }
}