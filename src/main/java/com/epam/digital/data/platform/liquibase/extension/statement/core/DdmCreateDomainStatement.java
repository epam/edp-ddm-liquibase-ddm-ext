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

package com.epam.digital.data.platform.liquibase.extension.statement.core;
import com.epam.digital.data.platform.liquibase.extension.change.DdmDomainConstraintConfig;
import liquibase.statement.AbstractSqlStatement;
import liquibase.statement.CompoundStatement;

import java.util.ArrayList;
import java.util.List;

public class DdmCreateDomainStatement extends AbstractSqlStatement implements CompoundStatement {
    private String name;
    private String type;
    private Boolean nullable;
    private String collation;
    private String defaultValue;
    private List<DdmDomainConstraintConfig> constraints;

    public DdmCreateDomainStatement(String name) {
        this.name = name;
        constraints = new ArrayList<>();
    }

    public DdmCreateDomainStatement(String name, String type) {
        this.name = name;
        this.type = type;
        constraints = new ArrayList<>();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Boolean getNullable() {
        return nullable;
    }

    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
    }

    public String getCollation() {
        return collation;
    }

    public void setCollation(String collation) {
        this.collation = collation;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public List<DdmDomainConstraintConfig> getConstraints() {
        return this.constraints;
    }

    public void setConstraints(List<DdmDomainConstraintConfig> constraints) {
        this.constraints = constraints;
    }

    public void addConstraint(DdmDomainConstraintConfig constraint) {
        this.constraints.add(constraint);
    }
}
