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
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmReferenceTableStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmReferenceTableChangeTest {
    private DdmReferenceTableChange change;

    @BeforeEach
    void setUp() {
        change = new DdmReferenceTableChange();
    }

    @Test
    @DisplayName("Check statements - scope=all")
    public void checkStatementsAll() {
        change.setScope("all");
        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(2, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmReferenceTableStatement);
        Assertions.assertTrue(statements[1] instanceof DdmReferenceTableStatement);
    }

    @Test
    @DisplayName("Check statements - scope=primary")
    public void checkStatementsPrimary() {
        change.setScope("primary");
        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(1, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmReferenceTableStatement);
    }

    @Test
    @DisplayName("Check statements - scope=history")
    public void checkStatementsHistory() {
        change.setScope("history");
        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(1, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmReferenceTableStatement);
    }

    @Test
    @DisplayName("Validate change")
    public void validateChange() {
        change.setScope("all");
        change.setTableName("name");
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - tableName is required")
    public void validateChangeName() {
        change.setScope("all");
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

}