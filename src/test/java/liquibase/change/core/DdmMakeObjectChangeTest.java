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
import liquibase.statement.core.AddColumnStatement;
import liquibase.statement.core.AddUniqueConstraintStatement;
import liquibase.statement.core.CreateTableStatement;
import liquibase.statement.core.DropUniqueConstraintStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.statement.core.RenameTableStatement;
import liquibase.structure.core.Column;
import liquibase.structure.core.DataType;
import liquibase.structure.core.Table;
import liquibase.structure.core.UniqueConstraint;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmMakeObjectChangeTest {
    private DdmMakeObjectChange change;
    private DdmMakeObjectChange snapshotChange;

    List<DdmTableConfig> createTables() {
        List<DdmTableConfig> tables = new ArrayList<>();

        DdmTableConfig table = new DdmTableConfig();
        table.setName("table");

        tables.add(table);

        return tables;
    }

    @BeforeEach
    void setUp() {
        change = new DdmMakeObjectChange();
        change.setTables(createTables());

        Table table = new Table();
        table.setName("table_hst");

        DataType colType = new DataType("text");
        Column column = new Column("column");
        column.setNullable(false);
        column.setType(colType);
        table.addColumn(column);

        UniqueConstraint constraint = new UniqueConstraint();
        constraint.setName("uniqueConstraint");
        constraint.addColumn(0, column);

        table.setAttribute("uniqueConstraints", asList(constraint));

        snapshotChange = new DdmMakeObjectChange(new DdmMockSnapshotGeneratorFactory(table));
        snapshotChange.setTables(createTables());
    }

    @Test
    @DisplayName("Check load makeObject")
    public void checkLoad() throws ChangeLogParseException, Exception {
        XMLChangeLogSAXParser xmlParser = new XMLChangeLogSAXParser();
        DdmResourceAccessor resourceAccessor = new DdmResourceAccessor();
        DatabaseChangeLog changeLog = xmlParser.parse(DdmTestConstants.TEST_MAKE_OBJECT_FILE_NAME,
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
    @DisplayName("Validate change")
    public void validateChange() {
        Assertions.assertEquals(0, snapshotChange.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Check statements")
    public void checkStatements() {
        SqlStatement[] statements = snapshotChange.generateStatements(new MockDatabase());
        Assertions.assertEquals(7, statements.length);
        Assertions.assertTrue(statements[0] instanceof AddColumnStatement);
        Assertions.assertTrue(statements[1] instanceof DropUniqueConstraintStatement);
        Assertions.assertTrue(statements[2] instanceof RenameTableStatement);
        Assertions.assertTrue(statements[3] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[4] instanceof AddUniqueConstraintStatement);
        Assertions.assertTrue(statements[5] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[6] instanceof RawSqlStatement);
    }

    @Test
    @DisplayName("Check messages")
    public void checkMessages() {
        Assertions.assertEquals("Objects have been made", snapshotChange.getConfirmationMessage());
        Assertions.assertEquals("http://www.liquibase.org/xml/ns/dbchangelog", snapshotChange.getSerializedObjectNamespace());
    }
}