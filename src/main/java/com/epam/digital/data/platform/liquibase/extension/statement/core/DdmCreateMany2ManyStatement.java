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

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import liquibase.statement.AbstractSqlStatement;
import liquibase.statement.CompoundStatement;

public class DdmCreateMany2ManyStatement extends AbstractSqlStatement implements CompoundStatement {

    private String mainTableName;
    private String mainTableKeyField;
    private String referenceTableName;
    private String referenceColumnName;
    private String referenceKeysArray;
    private List<DdmColumnConfig> mainTableColumns = new ArrayList<>();
    private List<DdmColumnConfig> referenceTableColumns = new ArrayList<>();


    public DdmCreateMany2ManyStatement() {
        super();
    }

    public String getName() {
        return mainTableName + "_" + referenceTableName;
    }

    public String getRelationName() {
        return getName() + DdmConstants.SUFFIX_RELATION;
    }

    public String getViewName() {
        return getRelationName() + DdmConstants.SUFFIX_VIEW;
    }

    public String getMainTableName() {
        return mainTableName;
    }

    public void setMainTableName(String mainTableName) {
        this.mainTableName = mainTableName;
    }

    public String getMainTableKeyField() {
        return mainTableKeyField;
    }

    public void setMainTableKeyField(String mainTableKeyField) {
        this.mainTableKeyField = mainTableKeyField;
    }

    public String getReferenceTableName() {
        return referenceTableName;
    }

    public void setReferenceTableName(String referenceTableName) {
        this.referenceTableName = referenceTableName;
    }

    public String getReferenceKeysArray() {
        return referenceKeysArray;
    }

    public void setReferenceKeysArray(String referenceKeysArray) {
        this.referenceKeysArray = referenceKeysArray;
    }

    public List<DdmColumnConfig> getMainTableColumns() {
        return mainTableColumns;
    }

    public void setMainTableColumns(List<DdmColumnConfig> mainTableColumns) {
        this.mainTableColumns = mainTableColumns;
    }

    public List<DdmColumnConfig> getReferenceTableColumns() {
        return referenceTableColumns;
    }

    public void setReferenceTableColumns(
        List<DdmColumnConfig> referenceTableColumns) {
        this.referenceTableColumns = referenceTableColumns;
    }

    public String getReferenceColumnName() {
        return referenceColumnName;
    }

    public void setReferenceColumnName(String referenceColumnName) {
        this.referenceColumnName = referenceColumnName;
    }
}
