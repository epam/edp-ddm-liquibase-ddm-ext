/*
 * Copyright 2021 EPAM Systems.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.epam.digital.data.platform.liquibase.extension.change.core;

import com.epam.digital.data.platform.liquibase.extension.DdmTest;
import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmLabelConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTypeConfig;
import liquibase.change.Change;
import liquibase.database.core.MockDatabase;
import liquibase.statement.SqlStatement;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateTypeStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

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
    @DisplayName("Check load")
    public void checkLoad() throws Exception {
        Assertions.assertEquals(1, DdmTest.loadChangeSets(DdmTest.TEST_CREATE_ENUM_TYPE_FILE_NAME).size());
        Assertions.assertEquals(1, DdmTest.loadChangeSets(DdmTest.TEST_CREATE_COMPOSITE_TYPE_FILE_NAME).size());
    }
}