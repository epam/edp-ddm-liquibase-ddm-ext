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
import java.util.Objects;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.serializer.AbstractLiquibaseSerializable;

import java.util.ArrayList;
import java.util.List;

/**
 * Creates a new role.
 */
public class DdmRoleConfig extends AbstractLiquibaseSerializable {

    private String name;
    private String realm;
    private List<DdmTableConfig> tables = new ArrayList<>();
    private List<DdmSearchConditionConfig> searchConditions;

    public DdmRoleConfig() {
        super();
    }

    @Override
    public String getSerializedObjectName() {
        return "ddmRole";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return GENERIC_CHANGELOG_EXTENSION_NAMESPACE;
    }

    public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
        for (ParsedNode child : parsedNode.getChildren()) {
            if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_NAME)) {
                setName(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_NAME, String.class));
            } else if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_REALM)) {
                setRealm(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_REALM, String.class));
            } else if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_TABLE) || child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_VIEW)) {
                DdmTableConfig table = new DdmTableConfig();
                table.load(child, resourceAccessor);
                addTable(table);
            }
        }
    }

    public static List<DdmSearchConditionConfig> loadSearchConditions(ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
        List<DdmSearchConditionConfig> searchConditionConfig = new ArrayList<>();
        if (Objects.nonNull(parsedNode)) {
            for (ParsedNode child : parsedNode.getChildren()) {
                if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_SEARCH_CONDITION)) {
                    DdmSearchConditionConfig searchCondition = new DdmSearchConditionConfig();
                    searchCondition.load(child, resourceAccessor);
                    searchConditionConfig.add(searchCondition);
                }
            }
        }
        return searchConditionConfig;
    }

    public String getRealmAndName() {
        return (getRealm() != null ? getRealm() + "." : "") + getName();
    }

    public List<DdmTableConfig> getTables() {
        return this.tables;
    }

    public List<DdmSearchConditionConfig> getSearchConditions() {
        return this.searchConditions;
    }

    public void setTables(List<DdmTableConfig> tables) {
        this.tables = tables;
    }
    public void setSearchConditions(List<DdmSearchConditionConfig> searchConditions) {
        this.searchConditions = searchConditions;
    }

    public void addTable(DdmTableConfig table) {
        this.tables.add(table);
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getRealm() {
        return realm;
    }

    public void setRealm(String realm) {
        this.realm = realm;
    }
}
