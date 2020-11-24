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

import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import liquibase.statement.AbstractSqlStatement;
import liquibase.statement.CompoundStatement;

public class DdmCreateSimpleSearchConditionStatement extends AbstractSqlStatement implements CompoundStatement {

    private String name;
    private DdmTableConfig table;
    private DdmColumnConfig searchColumn;
    private Boolean indexing;
    private String limit;

    public DdmCreateSimpleSearchConditionStatement(String name) {
        super();
        this.name = name;
    }

    public DdmTableConfig getTable() {
        return this.table;
    }

    public void setTable(DdmTableConfig table) {
        this.table = table;
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

    public DdmColumnConfig getSearchColumn() {
        return searchColumn;
    }

    public void setSearchColumn(DdmColumnConfig searchColumn) {
        this.searchColumn = searchColumn;
    }

    public String getLimit() {
        return limit;
    }

    public void setLimit(String limit) {
        this.limit = limit;
    }
}
