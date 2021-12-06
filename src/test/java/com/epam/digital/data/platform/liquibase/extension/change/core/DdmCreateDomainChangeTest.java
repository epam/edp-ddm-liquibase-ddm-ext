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

import com.epam.digital.data.platform.liquibase.extension.change.DdmDomainConstraintConfig;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateDomainStatement;
import liquibase.change.Change;
import liquibase.database.core.MockDatabase;
import liquibase.statement.SqlStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class DdmCreateDomainChangeTest {
    private DdmCreateDomainChange change;

    @BeforeEach
    void setUp() {
        change = new DdmCreateDomainChange();
    }

    @Test
    @DisplayName("Check statements")
    public void checkStatements() {
        SqlStatement[] statements = change.generateStatements(new MockDatabase());
        Assertions.assertEquals(1, statements.length);
        Assertions.assertTrue(statements[0] instanceof DdmCreateDomainStatement);
    }

    @Test
    @DisplayName("Validate change")
    public void validateChange() {
        change.setName("name");
        change.setType("type");
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - name is required")
    public void validateChangeName() {
        change.setType("type");
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - type is required")
    public void validateChangeType() {
        change.setName("name");
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - constraints")
    public void validateChangeConstraints() {
        change.setName("name");
        change.setType("type");
        change.addConstraint(new DdmDomainConstraintConfig("name", "implementation"));
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
        Assertions.assertEquals("name", change.getConstraints().get(0).getName());
    }

    @Test
    @DisplayName("Validate change - list constraints")
    public void validateChangeListConstraints() {
        change.setName("name");
        change.setType("type");
        List<DdmDomainConstraintConfig> constraints = new ArrayList<>();
        constraints.add(new DdmDomainConstraintConfig("name", "implementation"));
        change.setConstraints(constraints);
        Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
        Assertions.assertEquals("name", change.getConstraints().get(0).getName());
    }

    @Test
    @DisplayName("Check inverse")
    public void checkInverse() {
        Change[] changes = change.createInverses();
        Assertions.assertEquals(1, changes.length);
        Assertions.assertTrue(changes[0] instanceof DdmDropDomainChange);
    }

    @Test
    @DisplayName("Check confirmation message")
    public void checkConfirmationMessage() {
        change.setName("name");
        Assertions.assertEquals("Domain name created", change.getConfirmationMessage());

    }
}