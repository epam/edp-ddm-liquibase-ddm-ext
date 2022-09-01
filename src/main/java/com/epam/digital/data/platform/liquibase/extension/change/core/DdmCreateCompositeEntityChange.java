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
import com.epam.digital.data.platform.liquibase.extension.change.DdmLinkConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmNestedEntityConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.ColumnConfig;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.statement.SqlStatement;

@DatabaseChange(name = "createCompositeEntity", description = "Create Composite Entity", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class DdmCreateCompositeEntityChange extends AbstractChange {

  private String name;
  private List<DdmNestedEntityConfig> nestedEntities = new ArrayList<>();

  @Override
  public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor)
      throws ParsedNodeException {
    super.load(parsedNode, resourceAccessor);

    for (ParsedNode child : parsedNode.getChildren()) {
      if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_NAME)) {
        setName(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_NAME, String.class));
      } else {
        DdmNestedEntityConfig nestedEntity = new DdmNestedEntityConfig();
        nestedEntity.load(child, resourceAccessor);
        this.nestedEntities.add(nestedEntity);
      }
    }
  }

  @Override
  public ValidationErrors validate(Database database) {
    ValidationErrors validationErrors = new ValidationErrors();
    validationErrors.addAll(super.validate(database));
    List<String> missingTables = validateTables();
    if (!missingTables.isEmpty()) {
      validationErrors.addError("Missing required tables: " + missingTables);
      return validationErrors;
    }
    if (!isValidEntities()) {
      validationErrors.addError("Not enough required relations");
    }
    return validationErrors;
  }

  private List<String> validateTables() {
    List<String> requiredTables = nestedEntities.stream()
        .map(DdmNestedEntityConfig::getTable)
        .collect(Collectors.toList());
    List<String> requiredTablesInChangeLog = requiredTables.stream()
        .filter(this::tableExistsInChangeLog)
        .collect(Collectors.toList());
    return requiredTables.stream()
        .filter(requiredTable -> !requiredTablesInChangeLog.contains(requiredTable))
        .collect(Collectors.toList());
  }

  private boolean isValidEntities() {
    List<DdmNestedEntityConfig> requiredEntities = nestedEntities.stream()
        .filter(entity -> !entity.getLinkConfig().isEmpty())
        .collect(Collectors.toList());
    populateEntityTable(requiredEntities);
    List<DdmCreateTableChange> tableChangesWithLinks =
        DdmUtils.getCreateTableChangesFromChangeLog(this.getChangeSet(),
        requiredEntities.stream().map(DdmNestedEntityConfig::getTable)
            .collect(Collectors.toList()));
    List<DdmNestedEntityConfig> requiredEntitiesFromChangelog = tableChangesWithLinks.stream()
        .filter(tableChange -> isValidForeignKey(requiredEntities, tableChange))
        .map(this::convertChangeToConfig)
        .collect(Collectors.toList());
    return requiredEntitiesFromChangelog.containsAll(requiredEntities);
  }

  private boolean tableExistsInChangeLog(String tableName) {
    return this.getChangeSet().getChangeLog().getChangeSets().stream().anyMatch(
        changeSet -> changeSet.getChanges().stream()
            .filter(change -> change instanceof DdmCreateTableChange)
            .map(change -> (DdmCreateTableChange) change)
            .anyMatch(tableChange -> tableChange.getTableName().equals(tableName)));
  }

  private void populateEntityTable(List<DdmNestedEntityConfig> requiredEntities) {
    requiredEntities.stream().flatMap(requiredEntity -> requiredEntity.getLinkConfig().stream())
        .forEach(linkConfig -> {
          for (DdmNestedEntityConfig nestedEntity : nestedEntities) {
            if (linkConfig.getEntity().equals(nestedEntity.getName())) {
              linkConfig.setEntityTable(nestedEntity.getTable());
            }
          }
        });
  }

  private boolean isValidForeignKey(List<DdmNestedEntityConfig> requiredEntities,
      DdmCreateTableChange tableChange) {
    for (DdmNestedEntityConfig entity : requiredEntities) {
      if (entity.getTable().equals(tableChange.getTableName())) {
        int keyCount = 0;
        for (DdmLinkConfig linkConfig : entity.getLinkConfig()) {
          for (ColumnConfig column : tableChange.getColumns()) {
            if (column.getConstraints() != null &&
                column.getConstraints().getForeignKeyName() != null &&
                linkConfig.getColumn().equals(column.getName()) &&
                linkConfig.getEntityTable()
                    .equals(column.getConstraints().getReferencedTableName())) {
              keyCount++;
            }
          }
        }
        return entity.getLinkConfig().size() == keyCount;
      }
    }
    return false;
  }

  private DdmNestedEntityConfig convertChangeToConfig(DdmCreateTableChange change) {
    DdmNestedEntityConfig nestedEntity = new DdmNestedEntityConfig();

    List<String> linkColumns = nestedEntities.stream()
        .filter(entity -> entity.getTable().equals(change.getTableName()))
        .flatMap(entity -> entity.getLinkConfig().stream())
        .map(DdmLinkConfig::getColumn)
        .collect(Collectors.toList());

    List<DdmLinkConfig> links = new ArrayList<>();
    for (ColumnConfig column : change.getColumns()) {
      for (String linkColumn : linkColumns) {
        DdmLinkConfig link = new DdmLinkConfig();
        if (column.getConstraints() != null &&
            column.getConstraints().getForeignKeyName() != null &&
            column.getName().equals(linkColumn)) {
          link.setColumn(column.getName());
          link.setEntityTable(column.getConstraints().getReferencedTableName());
          links.add(link);
        }
      }
    }
    nestedEntity.setTable(change.getTableName());
    nestedEntity.setLinkConfig(links);
    return nestedEntity;
  }

  @Override
  public String getConfirmationMessage() {
    return "Composite entity created";
  }

  @Override
  public String getSerializedObjectNamespace() {
    return STANDARD_CHANGELOG_NAMESPACE;
  }

  @Override
  public SqlStatement[] generateStatements(Database database) {
    List<SqlStatement> statements = new ArrayList<>();
    List<DdmNestedEntityConfig> requiredEntities = nestedEntities.stream()
        .filter(entity -> entity.getLinkConfig() != null)
        .collect(Collectors.toList());
    for (DdmNestedEntityConfig entity : requiredEntities) {
      for (DdmLinkConfig linkConfig : entity.getLinkConfig()) {
        statements.add(DdmUtils.insertMetadataSql(DdmConstants.ATTRIBUTE_NESTED,
            getName(), entity.getTable(), linkConfig.getColumn()));
      }
    }
    return statements.toArray(new SqlStatement[0]);
  }

  public List<DdmNestedEntityConfig> getNestedEntities() {
    return nestedEntities;
  }

  public void setNestedEntities(List<DdmNestedEntityConfig> nestedEntities) {
    this.nestedEntities = nestedEntities;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}
