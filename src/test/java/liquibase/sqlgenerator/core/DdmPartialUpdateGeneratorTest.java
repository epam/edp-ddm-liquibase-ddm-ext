package liquibase.sqlgenerator.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import liquibase.DdmMockSnapshotGeneratorFactory;
import liquibase.change.DdmColumnConfig;
import liquibase.change.DdmTableConfig;
import liquibase.database.core.MockDatabase;
import liquibase.sql.Sql;
import liquibase.statement.core.DdmPartialUpdateStatement;
import liquibase.structure.core.Column;
import liquibase.structure.core.DataType;
import liquibase.structure.core.Table;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmPartialUpdateGeneratorTest {

    @Test
    @DisplayName("Validate SQL")
    public void validateSQL() {
        Table snapshotTable = new Table();
        snapshotTable.setName("table");

        Column snapshotColumn = new Column("column");
        snapshotColumn.setNullable(false);
        snapshotColumn.setType(new DataType("text"));
        snapshotTable.addColumn(snapshotColumn);

        DdmPartialUpdateGenerator generator = new DdmPartialUpdateGenerator(new DdmMockSnapshotGeneratorFactory(snapshotTable));
        DdmPartialUpdateStatement statement = new DdmPartialUpdateStatement("name");

        List<DdmTableConfig> tables = new ArrayList<>();
        DdmTableConfig table = new DdmTableConfig("table");

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column");
        table.addColumn(column);
        tables.add(table);

        statement.setTables(tables);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("insert into ddm_liquibase_metadata(change_type, change_name, attribute_name, attribute_value) values ('partialUpdate', 'name', 'table', 'column');", sqls[0].toSql());
    }

}