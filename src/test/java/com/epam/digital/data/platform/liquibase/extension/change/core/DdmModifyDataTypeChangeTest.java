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

import liquibase.database.core.MockDatabase;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.ModifyDataTypeStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmModifyDataTypeChangeTest {
    private DdmModifyDataTypeChange change;

    @BeforeEach
    void setUp() {
        change = new DdmModifyDataTypeChange();
        change.setTableName("table");
        change.setColumnName("column");
        change.setNewDataType("type");
    }

    @Test
    @DisplayName("Validate change")
    void validate() {
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - historyFlag=false")
    void validateHistoryFlagFalse() {
        change.setHistoryFlag(false);
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - historyFlag=true")
    void validateHistoryFlagTrue() {
        change.setHistoryFlag(true);
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());

        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(2, statements.length);
        Assertions.assertTrue(statements[0] instanceof ModifyDataTypeStatement);
        Assertions.assertTrue(statements[1] instanceof ModifyDataTypeStatement);
    }
}