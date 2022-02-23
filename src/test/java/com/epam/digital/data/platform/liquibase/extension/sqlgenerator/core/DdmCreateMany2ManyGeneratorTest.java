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

package com.epam.digital.data.platform.liquibase.extension.sqlgenerator.core;

import java.util.ArrayList;
import java.util.List;

import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateMany2ManyStatement;
import liquibase.database.core.MockDatabase;
import liquibase.sql.Sql;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DdmCreateMany2ManyGeneratorTest {

    private DdmCreateMany2ManyGenerator generator;
    private DdmCreateMany2ManyStatement statement;

    @BeforeEach
    void setUp() {
        generator = new DdmCreateMany2ManyGenerator();
        statement = new DdmCreateMany2ManyStatement();
    }

    @Test
    @DisplayName("Validate generator")
    public void validateGenerator() {
        statement.setMainTableName("mainTable");
        statement.setMainTableKeyField("keyField");
        statement.setReferenceTableName("referenceTable");
        statement.setReferenceColumnName("ref_id");
        statement.setReferenceKeysArray("keysArray");
        Assertions.assertEquals(0, generator.validate(statement, new MockDatabase(), null).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate generator - mainTableName is required")
    public void validateGeneratorMainTable() {
        statement.setMainTableKeyField("keyField");
        statement.setReferenceTableName("referenceTable");
        statement.setReferenceKeysArray("keysArray");
        Assertions.assertEquals(1, generator.validate(statement, new MockDatabase(), null).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate generator - mainTableKeyField is required")
    public void validateGeneratorKeyField() {
        statement.setMainTableName("mainTable");
        statement.setReferenceTableName("referenceTable");
        statement.setReferenceKeysArray("keysArray");
        Assertions.assertEquals(1, generator.validate(statement, new MockDatabase(), null).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate generator - referenceTableName is required")
    public void validateGeneratorReferenceTable() {
        statement.setMainTableName("mainTable");
        statement.setMainTableKeyField("keyField");
        statement.setReferenceKeysArray("keysArray");
        Assertions.assertEquals(1, generator.validate(statement, new MockDatabase(), null).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate generator - referenceKeysArray is required")
    public void validateGeneratorKeysArray() {
        statement.setMainTableName("mainTable");
        statement.setMainTableKeyField("keyField");
        statement.setReferenceTableName("referenceTable");
        Assertions.assertEquals(1, generator.validate(statement, new MockDatabase(), null).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate SQL")
    public void validateSQL() {
        statement.setMainTableName("mainTable");
        statement.setMainTableKeyField("keyField");
        statement.setReferenceTableName("referenceTable");
        statement.setReferenceColumnName("ref_id");
        statement.setReferenceKeysArray("keysArray");

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW mainTable_referenceTable_rel_v AS SELECT mainTable.keyField, UNNEST(mainTable.keysArray) AS ref_id FROM mainTable;" +
                "\n\n" +
                "CREATE INDEX ix_mainTable_referenceTable_m2m ON mainTable USING gin(keysArray);" +
                "\n\n" +
                "CREATE TRIGGER trg_referenceTable_integrity_mainTable_keysArray BEFORE UPDATE OR DELETE ON referenceTable FOR EACH ROW EXECUTE FUNCTION f_trg_check_m2m_integrity('ref_id', 'mainTable', 'keysArray');"
            , sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - additional columns from mainTable")
    public void validateSQLMain() {
        List<DdmColumnConfig> columns = new ArrayList<>();
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("Column1");
        columns.add(column);

        column = new DdmColumnConfig();
        column.setName("Column2");
        column.setAlias("col2");
        columns.add(column);

        statement.setMainTableName("mainTable");
        statement.setMainTableKeyField("keyField");
        statement.setReferenceTableName("referenceTable");
        statement.setReferenceColumnName("ref_id");
        statement.setReferenceKeysArray("keysArray");
        statement.setMainTableColumns(columns);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW mainTable_referenceTable_rel_v AS SELECT mainTable.keyField, UNNEST(mainTable.keysArray) AS ref_id, mainTable.Column1, mainTable.Column2 AS col2 FROM mainTable;" +
                "\n\n" +
                "CREATE INDEX ix_mainTable_referenceTable_m2m ON mainTable USING gin(keysArray);" +
                "\n\n" +
                "CREATE TRIGGER trg_referenceTable_integrity_mainTable_keysArray BEFORE UPDATE OR DELETE ON referenceTable FOR EACH ROW EXECUTE FUNCTION f_trg_check_m2m_integrity('ref_id', 'mainTable', 'keysArray');"
            , sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - additional columns from referenceTable")
    public void validateSQLReference() {
        List<DdmColumnConfig> columns = new ArrayList<>();
        DdmColumnConfig column = new DdmColumnConfig();
        column.setName("Column1");
        columns.add(column);

        column = new DdmColumnConfig();
        column.setName("Column2");
        column.setAlias("col2");
        columns.add(column);

        statement.setMainTableName("mainTable");
        statement.setMainTableKeyField("keyField");
        statement.setReferenceTableName("referenceTable");
        statement.setReferenceColumnName("ref_id");
        statement.setReferenceKeysArray("keysArray");
        statement.setReferenceTableColumns(columns);

        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE OR REPLACE VIEW mainTable_referenceTable_rel_v AS WITH main_cte as (SELECT mainTable.keyField, UNNEST(mainTable.keysArray) AS ref_id FROM mainTable) SELECT main_cte.keyField, main_cte.ref_id, referenceTable.Column1, referenceTable.Column2 AS col2 FROM main_cte JOIN referenceTable USING (ref_id);" +
                "\n\n" +
                "CREATE INDEX ix_mainTable_referenceTable_m2m ON mainTable USING gin(keysArray);" +
                "\n\n" +
                "CREATE TRIGGER trg_referenceTable_integrity_mainTable_keysArray BEFORE UPDATE OR DELETE ON referenceTable FOR EACH ROW EXECUTE FUNCTION f_trg_check_m2m_integrity('ref_id', 'mainTable', 'keysArray');"
            , sqls[0].toSql());
    }
}