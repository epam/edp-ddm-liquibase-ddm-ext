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

import com.epam.digital.data.platform.liquibase.extension.change.DdmDomainConstraintConfig;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateDomainStatement;
import liquibase.database.core.MockDatabase;
import liquibase.sql.Sql;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DdmCreateDomainGeneratorTest {
    private DdmCreateDomainGenerator generator;

    @BeforeEach
    void setUp() {
        generator = new DdmCreateDomainGenerator();
    }

    @Test
    @DisplayName("Validate generator")
    public void validateChange() {
        DdmCreateDomainStatement statement = new DdmCreateDomainStatement("name", "type");
        Assertions.assertEquals(0, generator.validate(statement, new MockDatabase(), null).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate generator - name is required")
    public void validateChangeNameCheck() {
        DdmCreateDomainStatement statement = new DdmCreateDomainStatement("type");
        Assertions.assertEquals(1, generator.validate(statement, new MockDatabase(), null).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate generator - type is required")
    public void validateChangeTypeCheck() {
        DdmCreateDomainStatement statement = new DdmCreateDomainStatement("name");
        Assertions.assertEquals(1, generator.validate(statement, new MockDatabase(), null).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate SQL")
    public void validateSQL() {
        DdmCreateDomainStatement statement = new DdmCreateDomainStatement("name", "type");
        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE DOMAIN name AS type;", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - null")
    public void validateSQLNull() {
        DdmCreateDomainStatement statement = new DdmCreateDomainStatement("name", "type");
        statement.addConstraint(new DdmDomainConstraintConfig("name", "implementation"));
        statement.setNullable(false);
        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE DOMAIN name AS type NOT NULL CONSTRAINT name CHECK (implementation);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - collation")
    public void validateSQLCollation() {
        DdmCreateDomainStatement statement = new DdmCreateDomainStatement("name", "type");
        statement.addConstraint(new DdmDomainConstraintConfig("name", "implementation"));
        statement.setCollation("collation");
        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE DOMAIN name AS type COLLATE \"collation\" CONSTRAINT name CHECK (implementation);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - default")
    public void validateSQLDefault() {
        DdmCreateDomainStatement statement = new DdmCreateDomainStatement("name", "type");
        statement.addConstraint(new DdmDomainConstraintConfig("name", "implementation"));
        statement.setDefaultValue("default");
        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE DOMAIN name AS type DEFAULT default CONSTRAINT name CHECK (implementation);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - constraints")
    public void validateSQLConstraints() {
        DdmCreateDomainStatement statement = new DdmCreateDomainStatement("name", "type");
        statement.addConstraint(new DdmDomainConstraintConfig("name", "implementation"));
        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE DOMAIN name AS type CONSTRAINT name CHECK (implementation);", sqls[0].toSql());
    }

    @Test
    @DisplayName("Validate SQL - constraints 2")
    public void validateSQLConstraints2() {
        DdmCreateDomainStatement statement = new DdmCreateDomainStatement("name", "type");
        DdmDomainConstraintConfig constraint = new DdmDomainConstraintConfig();
        constraint.setName("name");
        constraint.setImplementation("implementation");
        statement.addConstraint(constraint);
        Sql[] sqls = generator.generateSql(statement, new MockDatabase(), null);
        assertEquals("CREATE DOMAIN name AS type CONSTRAINT name CHECK (implementation);", sqls[0].toSql());
    }
}