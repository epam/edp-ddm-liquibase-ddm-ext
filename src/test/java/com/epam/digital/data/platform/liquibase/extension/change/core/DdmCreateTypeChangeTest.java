package com.epam.digital.data.platform.liquibase.extension.change.core;

import com.epam.digital.data.platform.liquibase.extension.DdmTestConstants;
import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmLabelConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTypeConfig;
import liquibase.*;
import liquibase.change.Change;
import com.epam.digital.data.platform.liquibase.extension.DdmResourceAccessor;
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
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateTypeStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

class DdmCreateTypeChangeTest {
    private DdmCreateTypeChange change;

    @BeforeEach
    void setUp() {
        change = new DdmCreateTypeChange();
    }

    @Test
    @DisplayName("Check statements")
    public void checkStatements() {
        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(1, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmCreateTypeStatement);
    }

    @Test
    @DisplayName("Validate change asComposite")
    public void validateChangeAsComposite() {
        change.setName("name");

        DdmTypeConfig asComposite = new DdmTypeConfig();

        List<DdmColumnConfig> columns = new ArrayList<>();
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column1");
        column.setType("type1");
        columns.add(column);

        column = new DdmColumnConfig();
        column.setName("column2");
        column.setType("type2");
        columns.add(column);

        asComposite.setColumns(columns);

        change.setAsComposite(asComposite);

        String name = asComposite.getSerializedObjectName();
        String namespace = asComposite.getSerializedObjectNamespace();
        String confirmation = change.getConfirmationMessage();

        Change[] changes = change.createInverses();

        Assertions.assertEquals("ddmType", name);
        Assertions.assertEquals("http://www.liquibase.org/xml/ns/dbchangelog-ext", namespace);
        Assertions.assertEquals("Type name created", confirmation);
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
        Assertions.assertEquals(0, changes[0].validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change asEnum")
    public void validateChangeAsEnum() {
        change.setName("name");

        DdmTypeConfig asEnum = new DdmTypeConfig();

        List<DdmLabelConfig> labels = new ArrayList<>();

        DdmLabelConfig label = new DdmLabelConfig();
        label.setLabel("label1");
        label.setTranslation("translation1");
        labels.add(label);

        label = new DdmLabelConfig();
        label.setLabel("label2");
        label.setTranslation("translation2");
        labels.add(label);

        asEnum.setLabels(labels);

        change.setAsEnum(asEnum);

        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Insert statement to metadata for enum")
    public void shouldAddInsertStatementForEnumLabels() {
        change.setName("name");

        DdmTypeConfig asEnum = new DdmTypeConfig();

        DdmLabelConfig label = new DdmLabelConfig();
        label.setLabel("label1");
        label.setTranslation("translation1");
        asEnum.addLabel(label);

        label = new DdmLabelConfig();
        label.setLabel("label2");
        label.setTranslation("translation2");
        asEnum.addLabel(label);

        change.setAsEnum(asEnum);
        SqlStatement[] statements = change.generateStatements(new MockDatabase());

        Assertions.assertEquals(5, statements.length);
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - type name is required")
    public void validateChangeTypeName() {

        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Check statements - asComposite adding columns")
    public void columns() {
        DdmTypeConfig asComposite = new DdmTypeConfig();

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column1");
        column.setType("type1");
        asComposite.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column2");
        column.setType("type2");
        asComposite.addColumn(column);

        change.setAsComposite(asComposite);

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        DdmCreateTypeStatement stmt = (DdmCreateTypeStatement)statements[0];
        Assertions.assertEquals(2, stmt.getAsComposite().getColumns().size());
    }

    @Test
    @DisplayName("Check statements - asEnum adding labels")
    public void labels() {
        DdmTypeConfig asEnum = new DdmTypeConfig();

        DdmLabelConfig label = new DdmLabelConfig();
        label.setLabel("label1");
        label.setTranslation("translation1");
        asEnum.addLabel(label);

        label = new DdmLabelConfig();
        label.setLabel("label2");
        label.setTranslation("translation2");
        asEnum.addLabel(label);

        change.setAsEnum(asEnum);

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        DdmCreateTypeStatement stmt = (DdmCreateTypeStatement)statements[0];
        Assertions.assertEquals(2, stmt.getAsEnum().getLabels().size());
    }

    @Test
    @DisplayName("Check load enum")
    public void checkLoadEnum() throws ChangeLogParseException, Exception {
        XMLChangeLogSAXParser xmlParser = new XMLChangeLogSAXParser();
        DdmResourceAccessor resourceAccessor = new DdmResourceAccessor();
        DatabaseChangeLog changeLog = xmlParser.parse(DdmTestConstants.TEST_CREATE_ENUM_TYPE_FILE_NAME,
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
    @DisplayName("Check load composite")
    public void checkLoadComposite() throws ChangeLogParseException, Exception {
        XMLChangeLogSAXParser xmlParser = new XMLChangeLogSAXParser();
        DdmResourceAccessor resourceAccessor = new DdmResourceAccessor();
        DatabaseChangeLog changeLog = xmlParser.parse(DdmTestConstants.TEST_CREATE_COMPOSITE_TYPE_FILE_NAME,
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