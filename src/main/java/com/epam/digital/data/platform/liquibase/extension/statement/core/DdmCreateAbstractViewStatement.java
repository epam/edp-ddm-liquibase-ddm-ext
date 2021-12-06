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

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.change.DdmConditionConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmCteConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmJoinConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import liquibase.statement.AbstractSqlStatement;
import liquibase.statement.CompoundStatement;

import java.util.ArrayList;
import java.util.List;

public class DdmCreateAbstractViewStatement extends AbstractSqlStatement implements CompoundStatement {

    private List<DdmCteConfig> ctes;
    private List<DdmTableConfig> tables;
    private List<DdmJoinConfig> joins;
    private String name;
    private Boolean indexing;
    private String limit;
    private List<DdmConditionConfig> conditions;

    public DdmCreateAbstractViewStatement(String name) {
        super();
        this.name = name;
        this.ctes = new ArrayList<>();
        this.tables = new ArrayList<>();
        this.joins = new ArrayList<>();
    }

    public String getViewName() {
        return this.name + DdmConstants.SUFFIX_VIEW;
    }

    public List<DdmCteConfig> getCtes() {
        return ctes;
    }

    public void setCtes(List<DdmCteConfig> ctes) {
        this.ctes = ctes;
    }

    public List<DdmTableConfig> getTables() {
        return this.tables;
    }

    public void setTables(List<DdmTableConfig> tables) {
        this.tables = tables;
    }

    public void addTable(DdmTableConfig table) {
        this.tables.add(table);
    }

    public List<DdmJoinConfig> getJoins() {
        return this.joins;
    }

    public void setJoins(List<DdmJoinConfig> joins) {
        this.joins = joins;
    }

    public void addJoin(DdmJoinConfig join) {
        this.joins.add(join);
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Boolean getIndexing() {
        return indexing;
    }

    public void setIndexing(Boolean indexing) {
        this.indexing = indexing;
    }

    public String getLimit() {
        return limit;
    }

    public void setLimit(String limit) {
        this.limit = limit;
    }

    public List<DdmConditionConfig> getConditions() {
        return conditions;
    }

    public void setConditions(List<DdmConditionConfig> conditions) {
        this.conditions = conditions;
    }
}
