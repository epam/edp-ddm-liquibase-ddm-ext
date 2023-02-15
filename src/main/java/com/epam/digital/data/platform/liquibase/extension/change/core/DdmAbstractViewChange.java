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

package com.epam.digital.data.platform.liquibase.extension.change.core;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.DdmUtils;
import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmConditionConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmCteConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmFunctionConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmJoinConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateAbstractViewStatement;
import java.util.stream.Collectors;
import liquibase.change.AbstractChange;
import liquibase.change.AddColumnConfig;
import liquibase.change.ColumnConfig;
import liquibase.change.core.AddColumnChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;

import java.util.ArrayList;
import java.util.List;

/**
 * Describes a parent entity for search conditions and analytics views.
 */
public abstract class DdmAbstractViewChange extends AbstractChange {

  private List<DdmCteConfig> ctes;
  private List<DdmTableConfig> tables;
  private List<DdmJoinConfig> joins;
  private String name;
  private Boolean indexing;
  private String limit;
  private String pagination;
  private List<DdmConditionConfig> conditions;

  public DdmAbstractViewChange() {
    super();
    this.ctes = new ArrayList<>();
    this.tables = new ArrayList<>();
    this.joins = new ArrayList<>();
  }

  public DdmAbstractViewChange(String name) {
    this();
    this.name = name.toLowerCase();
  }

  @Override
  public ValidationErrors validate(Database database) {
    ValidationErrors validationErrors = new ValidationErrors();
    validationErrors.addAll(super.validate(database));

    getJoins().stream().filter(join -> join.getLeftColumns().size() != join.getRightColumns().size())
        .map(join -> "Different amount of columns in join clause").forEach(validationErrors::addError);

    if (Boolean.TRUE.equals(getIndexing())) {
      boolean hasSearchColumns = false;

      for (DdmTableConfig table : getTables()) {
        if (table.getColumns().stream().anyMatch(column -> column.getSearchType() != null)) {
          hasSearchColumns = true;
        }
      }

      if (!hasSearchColumns) {
        validationErrors.addError("no search column is defined!");
      }
    }

    for (DdmTableConfig table : getTables()) {
      for (DdmFunctionConfig function : table.getFunctions()) {
        boolean isStringAgg = function.getName().equals(DdmConstants.ATTRIBUTE_FUNCTION_STRING_AGG);
        if (function.hasParameter() && !isStringAgg) {
          validationErrors.addError("function " + function.getName().toUpperCase() + " doesn't required additional parameter!");
        } 
        if (!function.hasParameter() && isStringAgg) {
          validationErrors.addError("function " + function.getName().toUpperCase() + " required additional parameter!");
        }
      }
    }
    return validationErrors;
  }

  protected DdmCreateAbstractViewStatement generateCreateAbstractViewStatement() {
    return new DdmCreateAbstractViewStatement(getName());
  }

  @Override
  public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
    super.load(parsedNode, resourceAccessor);

    for (ParsedNode child : parsedNode.getChildren()) {
      if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_CTE)) {
        DdmCteConfig cte = new DdmCteConfig();
        cte.load(child, resourceAccessor);
        addCte(cte);
      } else if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_TABLE)) {
        DdmTableConfig table = new DdmTableConfig();
        table.load(child, resourceAccessor);
        addTable(table);
      } else if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_JOIN)) {
        DdmJoinConfig join = new DdmJoinConfig();
        join.load(child, resourceAccessor);
        addJoin(join);
      } else if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_WHERE)) {
        setConditions(DdmConditionConfig.loadConditions(child, resourceAccessor));
      }
    }
  }

  public void updateColumnData() {
    List<String> tableNames = getTables().stream().map(DdmTableConfig::getName)
        .collect(Collectors.toList());
    List<DdmCreateTableChange> tableChanges =
        DdmUtils.getCreateTableChangesFromChangeLog(this.getChangeSet(), tableNames);
    List<AddColumnChange> columnChanges =
        DdmUtils.getColumnChangesFromChangeLog(this.getChangeSet(), tableNames);

    updateColumnDataFromCreateTableChanges(tableChanges);
    updateColumnDataFromAddColumnChanges(columnChanges);
  }

  private void updateColumnDataFromCreateTableChanges(List<DdmCreateTableChange> tableChanges) {
    for (DdmTableConfig table : getTables()) {
      for (DdmCreateTableChange tableChange : tableChanges) {
        if (tableChange.getTableName().equals(table.getName())) {
          for (DdmColumnConfig tableColumn : table.getColumns()) {
            for (ColumnConfig changeColumn : tableChange.getColumns()) {
              if (tableColumn.getName().equals(changeColumn.getName())) {
                tableColumn.setType(changeColumn.getType().toLowerCase());
                tableColumn.setConstraints(changeColumn.getConstraints());
              }
            }
          }
        }
      }
    }
  }

  private void updateColumnDataFromAddColumnChanges(List<AddColumnChange> columnChanges) {
    for (DdmTableConfig table : getTables()) {
      for (AddColumnChange columnChange : columnChanges) {
        if (columnChange.getTableName().equals(table.getName())) {
          for (DdmColumnConfig tableColumn : table.getColumns()) {
            for (AddColumnConfig changeColumn : columnChange.getColumns()) {
              if (tableColumn.getName().equals(changeColumn.getName())) {
                tableColumn.setType(changeColumn.getType().toLowerCase());
                tableColumn.setConstraints(changeColumn.getConstraints());
              }
            }
          }
        }
      }
    }
  }

  public List<DdmCteConfig> getCtes() {
    return ctes;
  }

  public void setCtes(List<DdmCteConfig> ctes) {
    this.ctes = ctes;
  }

  public void addCte(DdmCteConfig cte) {
    this.ctes.add(cte);
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
    this.name = name.toLowerCase();
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

  public String getPagination() {
    return pagination;
  }

  public void setPagination(String pagination) {
    this.pagination = pagination;
  }
}
