package liquibase.change.core;

import liquibase.DdmMockSnapshotGeneratorFactory;
import liquibase.change.AddColumnConfig;
import liquibase.change.ConstraintsConfig;
import liquibase.database.core.MockDatabase;
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

import static java.util.Arrays.asList;

class DdmAddColumnChangeTest {
    private DdmAddColumnChange change;
    private DdmAddColumnChange snapshotChange;

    @BeforeEach
    void setUp() {
        change = new DdmAddColumnChange();
        change.setTableName("table");

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

        snapshotChange = new DdmAddColumnChange(new DdmMockSnapshotGeneratorFactory(table));
        snapshotChange.setTableName("table");
    }

    @Test
    @DisplayName("Validate change")
    public void validateChange() {
        AddColumnConfig column1 = new AddColumnConfig();
        column1.setName("column1");
        column1.setType("type1");
        column1.setDefaultValue("defaultValue");
        ConstraintsConfig constraint = new ConstraintsConfig();
        constraint.setNullable("true");
        column1.setConstraints(constraint);
        change.addColumn(column1);

        AddColumnConfig column2 = new AddColumnConfig();
        column2.setName("column2");
        column2.setType("type2");
        change.addColumn(column2);

        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - default value")
    public void validateChangeDefaultValue() {
        AddColumnConfig column1 = new AddColumnConfig();
        column1.setName("column1");
        column1.setType("type1");
        ConstraintsConfig constraint = new ConstraintsConfig();
        constraint.setNullable("false");
        column1.setConstraints(constraint);
        change.addColumn(column1);

        AddColumnConfig column2 = new AddColumnConfig();
        column2.setName("column2");
        column2.setType("type2");
        change.addColumn(column2);

        Assertions.assertEquals(2, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Check statements")
    public void checkStatements() {
        snapshotChange.setHistoryFlag(true);

        AddColumnConfig column1 = new AddColumnConfig();
        column1.setName("column1");
        column1.setType("type1");
        column1.setDefaultValue("defaultValue");
        ConstraintsConfig constraint = new ConstraintsConfig();
        constraint.setNullable("true");
        column1.setConstraints(constraint);
        snapshotChange.addColumn(column1);

        AddColumnConfig column2 = new AddColumnConfig();
        column2.setName("column2");
        column2.setType("type2");
        snapshotChange.addColumn(column2);

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
    @DisplayName("Check snapshot")
    public void checkSnapshot() {
        snapshotChange.setHistoryFlag(true);

        MockDatabase database = new MockDatabase();

        SqlStatement[] statements = snapshotChange.generateStatements(database);
        Assertions.assertEquals(7, statements.length);
        Assertions.assertTrue(statements[0] instanceof AddColumnStatement);
        Assertions.assertTrue(statements[1] instanceof DropUniqueConstraintStatement);
        Assertions.assertTrue(statements[2] instanceof RenameTableStatement);
        Assertions.assertTrue(statements[3] instanceof CreateTableStatement);
        Assertions.assertTrue(statements[4] instanceof AddUniqueConstraintStatement);
        Assertions.assertTrue(statements[5] instanceof RawSqlStatement);
        Assertions.assertTrue(statements[6] instanceof RawSqlStatement);
    }

}