package liquibase.change.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import liquibase.Contexts;
import liquibase.DdmResourceAccessor;
import liquibase.DdmTestConstants;
import liquibase.LabelExpression;
import liquibase.RuntimeEnvironment;
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
import liquibase.statement.core.DdmCreateMany2ManyStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmCreateMany2ManyChangeTest {
    private DdmCreateMany2ManyChange change;

    @BeforeEach
    void setUp() {
        change = new DdmCreateMany2ManyChange();
    }

    @Test
    @DisplayName("Check statements")
    public void checkStatements() {
        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(1, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmCreateMany2ManyStatement);
    }

    @Test
    @DisplayName("Validate change")
    public void validateChange() {
        change.setMainTableName("mainTable");
        change.setMainTableKeyField("keyField");
        change.setReferenceTableName("referenceTable");
        change.setReferenceKeysArray("keysArray");
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - mainTableName is required")
    public void validateChangeMainTable() {
        change.setMainTableKeyField("keyField");
        change.setReferenceTableName("referenceTable");
        change.setReferenceKeysArray("keysArray");
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - mainTableKeyField is required")
    public void validateChangeKeyField() {
        change.setMainTableName("mainTable");
        change.setReferenceTableName("referenceTable");
        change.setReferenceKeysArray("keysArray");
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - referenceTableName is required")
    public void validateChangeReferenceTable() {
        change.setMainTableName("mainTable");
        change.setMainTableKeyField("keyField");
        change.setReferenceKeysArray("keysArray");
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - referenceKeysArray is required")
    public void validateChangeKeysArray() {
        change.setMainTableName("mainTable");
        change.setMainTableKeyField("keyField");
        change.setReferenceTableName("referenceTable");
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Check load")
    public void checkLoad() throws ChangeLogParseException, Exception {
        XMLChangeLogSAXParser xmlParser = new XMLChangeLogSAXParser();
        DdmResourceAccessor resourceAccessor = new DdmResourceAccessor();
        DatabaseChangeLog changeLog = xmlParser.parse(DdmTestConstants.TEST_CREATE_M2M_FILE_NAME,
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