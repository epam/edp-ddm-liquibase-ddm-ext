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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import liquibase.database.core.MockDatabase;
import liquibase.statement.SqlStatement;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmDropTypeStatement;
import liquibase.statement.core.RawSqlStatement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmDropTypeChangeTest {
    private DdmDropTypeChange change;

    @BeforeEach
    void setUp() {
        change = new DdmDropTypeChange();
    }

    @Test
    @DisplayName("Check statements")
    void checkStatements() {
        change.setName("someChangeName");
        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        assertEquals(3, statements.length);
        assertTrue(statements[0] instanceof DdmDropTypeStatement);
        assertEquals("delete from ddm_liquibase_metadata where change_type = 'type' and change_name = 'someChangeName';\n\n", ((RawSqlStatement) statements[1]).getSql());
        assertEquals("delete from ddm_liquibase_metadata where change_type = 'label' and change_name = 'someChangeName';\n\n", ((RawSqlStatement) statements[2]).getSql());
    }

    @Test
    @DisplayName("Validate change")
    void validateChange() {
        change.setName("name");
        assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - name is required")
    void validateChangeName() {
        assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }
}
