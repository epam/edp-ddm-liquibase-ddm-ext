package liquibase.change.core;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import liquibase.Contexts;
import liquibase.DdmMockSnapshotGeneratorFactory;
import liquibase.DdmResourceAccessor;
import liquibase.DdmTestConstants;
import liquibase.LabelExpression;
import liquibase.RuntimeEnvironment;
import liquibase.change.DdmColumnConfig;
import liquibase.change.DdmTableConfig;
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
import liquibase.statement.core.DdmPartialUpdateStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmPartialUpdateChangeTest {
    private DdmPartialUpdateChange change;

    @BeforeEach
    void setUp() {
        change = new DdmPartialUpdateChange();
    }

    @Test
    @DisplayName("Validate change")
    public void validateChange() {
        DdmTableConfig table = new DdmTableConfig();
        table.setName("name");

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column1");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column2");
        table.addColumn(column);

        change.addTable(table);

        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - table added twice")
    public void validateChangeTwice() {
        DdmTableConfig table = new DdmTableConfig();
        table.setName("name");

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column1");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column2");
        table.addColumn(column);

        change.addTable(table);
        change.addTable(table);

        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Check load partial update")
    public void checkLoad() throws ChangeLogParseException, Exception {
        XMLChangeLogSAXParser xmlParser = new XMLChangeLogSAXParser();
        DdmResourceAccessor resourceAccessor = new DdmResourceAccessor();
        DatabaseChangeLog changeLog = xmlParser.parse(DdmTestConstants.TEST_PARTIAL_UPDATE_FILE_NAME,
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

    @Test
    @DisplayName("Check statements")
    public void checkStatements() {
        List<DdmTableConfig> tables = new ArrayList<>();
        DdmTableConfig table = new DdmTableConfig("table");

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column1");
        table.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column2");
        table.addColumn(column);

        tables.add(table);
        change.setTables(tables);

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(1, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmPartialUpdateStatement);
    }

    @Test
    @DisplayName("Confirmation Message")
    public void confirmationMessage() {
        Assertions.assertEquals("Partial update has been set", change.getConfirmationMessage());
    }
}