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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;

import com.epam.digital.data.platform.liquibase.extension.DdmMockSnapshotGeneratorFactory;
import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmPartialUpdateStatement;
import liquibase.database.core.MockDatabase;
import liquibase.sql.Sql;
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