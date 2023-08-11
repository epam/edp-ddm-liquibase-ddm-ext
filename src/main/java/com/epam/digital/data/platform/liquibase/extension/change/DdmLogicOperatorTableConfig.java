/*
 * Copyright 2023 EPAM Systems.
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

import liquibase.serializer.AbstractLiquibaseSerializable;

import java.util.List;

public class DdmLogicOperatorTableConfig extends AbstractLiquibaseSerializable {

    private String tableName;
    private List<DdmLogicOperatorConfig> logicOperators;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<DdmLogicOperatorConfig> getLogicOperators() {
        return logicOperators;
    }

    public void setLogicOperators(
            List<DdmLogicOperatorConfig> logicOperators) {
        this.logicOperators = logicOperators;
    }

    @Override
    public String getSerializedObjectName() {
        return "ddmSearchConditionLogicOperator";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return GENERIC_CHANGELOG_EXTENSION_NAMESPACE;
    }
}
