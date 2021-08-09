package com.epam.digital.data.platform.liquibase.extension.sqlgenerator.core;

import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmLabelConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTypeConfig;
import liquibase.database.core.MockDatabase;
import liquibase.sql.Sql;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateTypeStatement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DdmCreateTypeGeneratorTest {
    private DdmCreateTypeGenerator generator;

    @BeforeEach
    void setUp() {
        generator = new DdmCreateTypeGenerator();
    }

    @Test
    @DisplayName("Validate change asComposite")
    public void validateChangeAsComposite() {
        DdmTypeConfig asComposite = new DdmTypeConfig();

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column1");
        column.setType("type1");
        asComposite.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column2");
        column.setType("type2");
        asComposite.addColumn(column);

        DdmCreateTypeStatement statement = new DdmCreateTypeStatement("name", asComposite, null);

        assertEquals(0, generator.validate(statement, new MockDatabase(), null).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change asComposite - columns are required")
    public void validateChangeColumns() {
        DdmTypeConfig asComposite = new DdmTypeConfig();
        DdmCreateTypeStatement statement = new DdmCreateTypeStatement("name", asComposite, null);
        assertEquals(1, generator.validate(statement, new MockDatabase(), null).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate SQL asComposite ")
    public void validateSQLAsComposite() {
        DdmTypeConfig asComposite = new DdmTypeConfig();

        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("column1");
        column.setType("type1");
        asComposite.addColumn(column);

        column = new DdmColumnConfig();
        column.setName("column2");
        column.setType("type2");
        column.setCollation("collation");
        asComposite.addColumn(column);

        DdmCreateTypeStatement statement = new DdmCreateTypeStatement("name", asComposite, null);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE TYPE name as (column1 type1, column2 type2 COLLATE \"collation\");", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate change asEnum")
    public void validateChangeAsEnum() {
        DdmTypeConfig asEnum = new DdmTypeConfig();

        DdmLabelConfig label = new DdmLabelConfig();
        label.setLabel("label1");
        label.setTranslation("translation1");
        asEnum.addLabel(label);

        label = new DdmLabelConfig();
        label.setLabel("label2");
        label.setTranslation("translation2");
        asEnum.addLabel(label);

        DdmCreateTypeStatement statement = new DdmCreateTypeStatement("name", null, asEnum);

        assertEquals(0, generator.validate(statement, new MockDatabase(), null).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change asEnum - labels are required")
    public void validateChangeLabels() {
        DdmTypeConfig asEnum = new DdmTypeConfig();
        DdmCreateTypeStatement statement = new DdmCreateTypeStatement("name", null, asEnum);
        assertEquals(1, generator.validate(statement, new MockDatabase(), null).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate SQL asEnum ")
    public void validateSQL() {
        DdmTypeConfig asEnum = new DdmTypeConfig();

        DdmLabelConfig label = new DdmLabelConfig();
        label.setLabel("label1");
        label.setTranslation("translation1");
        asEnum.addLabel(label);

        label = new DdmLabelConfig();
        label.setLabel("label2");
        label.setTranslation("translation2");
        asEnum.addLabel(label);

        DdmCreateTypeStatement statement = new DdmCreateTypeStatement("name", null, asEnum);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE TYPE name as ENUM ('label1', 'label2');", sqls[0].toSql());
    }

}