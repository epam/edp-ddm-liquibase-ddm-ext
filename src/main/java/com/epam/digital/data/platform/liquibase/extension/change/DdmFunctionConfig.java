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

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.serializer.AbstractLiquibaseSerializable;
import liquibase.util.StringUtil;

public class DdmFunctionConfig extends AbstractLiquibaseSerializable {
    private String name;
    private String tableAlias;
    private String columnName;
    private String alias;
    private String parameter;

    public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
        setName(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_NAME, String.class));
        setAlias(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_ALIAS, String.class));
        setColumnName(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_COLUMN_NAME, String.class));
        setParameter(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_PARAMETER, String.class));
    }

    public boolean hasTableAlias() {
        return !StringUtil.isEmpty(getTableAlias());
    }

    public boolean hasParameter() {
        return !StringUtil.isEmpty(getParameter());
    }

    @Override
    public String getSerializedObjectName() {
        return "ddmFunction";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return GENERIC_CHANGELOG_EXTENSION_NAMESPACE;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public void setTableAlias(String tableAlias) {
        this.tableAlias = tableAlias;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getParameter() {
        return parameter;
    }

    public void setParameter(String parameter) {
        this.parameter = parameter;
    }
}
