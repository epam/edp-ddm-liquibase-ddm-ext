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

import java.util.ArrayList;
import java.util.List;

import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import liquibase.statement.AbstractSqlStatement;

public class DdmPartialUpdateStatement extends AbstractSqlStatement {
    private List<DdmTableConfig> tables;
    private String name;

    public DdmPartialUpdateStatement(String name) {
        this.tables = new ArrayList<>();
        this.name = name;
    }

    public DdmPartialUpdateStatement() {
        this.tables = new ArrayList<>();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<DdmTableConfig> getTables() {
        return tables;
    }

    public void setTables(List<DdmTableConfig> tables) {
        this.tables = tables;
    }
}
