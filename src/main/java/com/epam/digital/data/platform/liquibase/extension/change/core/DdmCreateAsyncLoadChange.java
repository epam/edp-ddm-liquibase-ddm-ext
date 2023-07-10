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

package com.epam.digital.data.platform.liquibase.extension.change.core;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.DdmUtils;
import com.epam.digital.data.platform.liquibase.extension.change.DdmEntityConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.statement.SqlStatement;

/**
 * Creates a records in metadata table, which contains names of entities which may be uploaded asynchronously .
 */

@DatabaseChange(name = "createAsyncLoad", description = "Create Async Load", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class DdmCreateAsyncLoadChange extends AbstractChange {

  private String name;
  private List<DdmEntityConfig> entityList = new ArrayList<>();

  @Override
  public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor)
      throws ParsedNodeException {
    for (ParsedNode child : parsedNode.getChildren()) {
      if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_NAME)) {
        setName(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_NAME, String.class));
      } else {
        for(ParsedNode childItem : child.getChildren()) {
          DdmEntityConfig entity = new DdmEntityConfig();
          entity.load(childItem, resourceAccessor);
          this.entityList.add(entity);
        }
      }
    }
  }

  @Override
  public ValidationErrors validate(Database database) {
    ValidationErrors validationErrors = new ValidationErrors();
    validationErrors.addAll(super.validate(database));
    return validationErrors;
  }

  @Override
  public String getConfirmationMessage() {
    return "Async load created";
  }

  @Override
  public String getSerializedObjectNamespace() {
    return STANDARD_CHANGELOG_NAMESPACE;
  }

  @Override
  public SqlStatement[] generateStatements(Database database) {
    List<SqlStatement> statements = new ArrayList<>();
    List<DdmEntityConfig> requiredEntities = entityList.stream()
        .filter(entity -> entity.getLimit() != null)
        .collect(Collectors.toList());
    for (DdmEntityConfig entity : requiredEntities) {
        statements.add(DdmUtils.insertMetadataSql(DdmConstants.CREATE_ASYNC_LOAD,
          getName(), entity.getName(), entity.getLimit()));
    }
    return statements.toArray(new SqlStatement[0]);
  }

  public List<DdmEntityConfig> getEntityList() {
    return entityList;
  }

  public void setEntityList(List<DdmEntityConfig> entityList) {
    this.entityList = entityList;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

}
