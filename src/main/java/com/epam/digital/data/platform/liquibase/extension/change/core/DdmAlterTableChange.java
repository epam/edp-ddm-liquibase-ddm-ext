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

import static com.epam.digital.data.platform.liquibase.extension.DdmConstants.ATTRIBUTE_ASYNC;
import static com.epam.digital.data.platform.liquibase.extension.DdmConstants.ATTRIBUTE_BULK_LOAD;
import static com.epam.digital.data.platform.liquibase.extension.DdmConstants.ATTRIBUTE_FALSE;
import static com.epam.digital.data.platform.liquibase.extension.DdmConstants.ATTRIBUTE_SYNC;
import static com.epam.digital.data.platform.liquibase.extension.DdmConstants.ATTRIBUTE_TRUE;
import static com.epam.digital.data.platform.liquibase.extension.DdmConstants.CREATE_TABLE_CHANGE_NAME;
import static com.epam.digital.data.platform.liquibase.extension.DdmConstants.READ_MODE_CHANGE_TYPE;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.DdmUtils;
import com.epam.digital.data.platform.liquibase.extension.change.DdmAlterTableAttrConfig;
import java.util.ArrayList;
import java.util.List;
import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;

/**
 * Changing the value of the attributes (readMode, bulkLoad) after creating the table.
 */
@DatabaseChange(name = "alterTableApi", description = "Update attribute values", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class DdmAlterTableChange extends AbstractChange {

  private String table;
  private List<DdmAlterTableAttrConfig> attributeList = new ArrayList<>();

  @Override
  public SqlStatement[] generateStatements(Database database) {
    List<SqlStatement> statements = new ArrayList<>();
    for (DdmAlterTableAttrConfig attr : getAttributeList()) {
      String name = attr.getName();
      String value = attr.getValue();
      if (ATTRIBUTE_BULK_LOAD.equals(name)) {
        RawSqlStatement deleteBulkLoadStatement = DdmUtils.deleteMetadataByChangeTypeChangeNameAttrNameSql(
            ATTRIBUTE_BULK_LOAD, getTable(), ATTRIBUTE_BULK_LOAD);
        statements.add(deleteBulkLoadStatement);
        statements.addAll(insertStatementForBulkLoad(value));
      } else if (READ_MODE_CHANGE_TYPE.equals(name)) {
        RawSqlStatement deleteReadModeStatement = DdmUtils.deleteMetadataByChangeTypeChangeNameAttrNameSql(
            READ_MODE_CHANGE_TYPE, CREATE_TABLE_CHANGE_NAME, getTable());
        statements.add(deleteReadModeStatement);
        statements.addAll(insertStatementForReadMode(value));
      }
    }
    return statements.toArray(new SqlStatement[0]);
  }

  @Override
  public ValidationErrors validate(Database database) {
    ValidationErrors validationErrors = new ValidationErrors();
    checkTableExistence(validationErrors);
    for (DdmAlterTableAttrConfig attr : getAttributeList()) {
      String name = attr.getName();
      String value = attr.getValue();
      if (ATTRIBUTE_BULK_LOAD.equals(name)) {
        if (!ATTRIBUTE_TRUE.equals(value) && !ATTRIBUTE_FALSE.equals(value)) {
          validationErrors.addError(generateErrorMessage(name, value));
        }
      } else if (READ_MODE_CHANGE_TYPE.equals(name)) {
        if (!ATTRIBUTE_ASYNC.equals(value) && !ATTRIBUTE_SYNC.equals(value)) {
          validationErrors.addError(generateErrorMessage(name, value));
        }
      }
    }
    return validationErrors;
  }

  @Override
  public void load(ParsedNode node, ResourceAccessor resourceAccessor) throws ParsedNodeException {
    super.load(node, resourceAccessor);

    for (ParsedNode child : node.getChildren()) {
      if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE)) {
        DdmAlterTableAttrConfig attr = new DdmAlterTableAttrConfig();
        attr.load(child, resourceAccessor);
        getAttributeList().add(attr);
      }
    }
  }

  @Override
  public String getConfirmationMessage() {
    return String.format("Updated metadata for table [%s]", getTable());
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public List<DdmAlterTableAttrConfig> getAttributeList() {
    return attributeList;
  }

  public void setAttributeList(List<DdmAlterTableAttrConfig> attributeList) {
    this.attributeList = attributeList;
  }

  private void checkTableExistence(ValidationErrors validationErrors) {
    if (!DdmUtils.tableExistsInChangeLog(this.getChangeSet(), getTable())) {
      validationErrors.addError(String.format("Table [%s] does not exist", getTable()));
    }
  }

  private String generateErrorMessage(String name, String value) {
    return String.format("Attribute [%s] has an invalid value [%s]", name, value);
  }

  private List<SqlStatement> insertStatementForReadMode(String readModeValue) {
    List<SqlStatement> statements = new ArrayList<>();
    if (ATTRIBUTE_ASYNC.equals(readModeValue)) {
      statements.add(
          DdmUtils.insertMetadataSql(READ_MODE_CHANGE_TYPE, CREATE_TABLE_CHANGE_NAME, getTable(), readModeValue));
    }
    return statements;
  }

  private List<SqlStatement> insertStatementForBulkLoad(String bulkLoadValue) {
    List<SqlStatement> statements = new ArrayList<>();
    if (ATTRIBUTE_TRUE.equals(bulkLoadValue)) {
      statements.add(
          DdmUtils.insertMetadataSql(ATTRIBUTE_BULK_LOAD, getTable(), ATTRIBUTE_BULK_LOAD, bulkLoadValue));
    }
    return statements;
  }
}