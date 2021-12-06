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

package com.epam.digital.data.platform.liquibase.extension.change;

import liquibase.change.DatabaseChangeProperty;
import liquibase.serializer.AbstractLiquibaseSerializable;

public class DdmDomainConstraintConfig extends AbstractLiquibaseSerializable {
    private String name;
    private String implementation;

    public DdmDomainConstraintConfig() {
    }

    public DdmDomainConstraintConfig(String name, String implementation) {
        this.name = name;
        this.implementation = implementation;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @DatabaseChangeProperty(requiredForDatabase = "all")
    public String getImplementation() {
        return implementation;
    }

    public void setImplementation(String implementation) {
        this.implementation = implementation;
    }

    @Override
    public String getSerializedObjectName() {
        return "ddmDomainConstraint";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return GENERIC_CHANGELOG_EXTENSION_NAMESPACE;
    }
}
