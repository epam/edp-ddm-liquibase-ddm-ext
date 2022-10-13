/*
 * Copyright 2022 EPAM Systems.
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
import com.epam.digital.data.platform.liquibase.extension.change.*;
import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;

import java.util.ArrayList;
import java.util.List;

@DatabaseChange(name="rls", description = "Create RLS rules", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class DdmRlsRuleChange extends AbstractChange {

  private String name;

  private List<DdmRlsAddReadRuleConfig> addReadRules = new ArrayList<>();
  private List<DdmRlsAddWriteRuleConfig> addWriteRules = new ArrayList<>();
  private List<DdmRlsRemoveReadRuleConfig> removeReadRules = new ArrayList<>();
  private List<DdmRlsRemoveWriteRuleConfig> removeWriteRules = new ArrayList<>();


  @Override
  public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
    super.load(parsedNode, resourceAccessor);

    setName(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_NAME, String.class));

    for (ParsedNode child : parsedNode.getChildren()) {
      if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_ADD_READ_RULE)) {
        DdmRlsAddReadRuleConfig d = new DdmRlsAddReadRuleConfig();
        d.load(child, resourceAccessor);
        addReadRules.add(d);
      } else if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_ADD_WRITE_RULE)) {
        DdmRlsAddWriteRuleConfig d = new DdmRlsAddWriteRuleConfig();
        d.load(child, resourceAccessor);
        addWriteRules.add(d);
      } else if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_REMOVE_READ_RULE)) {
        DdmRlsRemoveReadRuleConfig d = new DdmRlsRemoveReadRuleConfig();
        d.load(child, resourceAccessor);
        removeReadRules.add(d);
      } else if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_REMOVE_WRITE_RULE)) {
        DdmRlsRemoveWriteRuleConfig d = new DdmRlsRemoveWriteRuleConfig();
        d.load(child, resourceAccessor);
        removeWriteRules.add(d);
      }
    }
  }

  @Override
  public String getConfirmationMessage() {
    return "RLS rules " + name + " created";
  }

  @Override
  public SqlStatement[] generateStatements(Database database) {
    List<SqlStatement> statements = new ArrayList<>();

    addReadRules.stream().forEach(e -> statements.add(insertMetadataSql(e)));
    addWriteRules.stream().forEach(e -> statements.add(insertMetadataSql(e)));
    removeReadRules.stream().forEach(e -> statements.add(deleteMetadataSql(e)));
    removeWriteRules.stream().forEach(e -> statements.add(deleteMetadataSql(e)));

    return statements.toArray(new SqlStatement[0]);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<DdmRlsAddReadRuleConfig> getAddReadRules() {
    return addReadRules;
  }

  public List<DdmRlsAddWriteRuleConfig> getAddWriteRules() {
    return addWriteRules;
  }

  public List<DdmRlsRemoveReadRuleConfig> getRemoveReadRules() {
    return removeReadRules;
  }

  public List<DdmRlsRemoveWriteRuleConfig> getRemoveWriteRules() {
    return removeWriteRules;
  }

  private RawSqlStatement insertMetadataSql(DdmRlsAddReadRuleConfig config) {
    return new RawSqlStatement("INSERT INTO " + DdmConstants.METADATA_RLS_TABLE + "(" +
            DdmConstants.METADATA_FIELD_TYPE + ", " +
            DdmConstants.METADATA_FIELD_NAME + ", " +
            DdmConstants.METADATA_FIELD_JWT_ATTRIBUTE + ", " +
            DdmConstants.METADATA_FIELD_CHECK_TABLE + ", " +
            DdmConstants.METADATA_FIELD_CHECK_COLUMN +
            ") VALUES (" +
            safeString(DdmConstants.METADATA_TYPE_READ) + ", " +
            safeString(config.getName()) + ", " +
            safeString(config.getJwtAttribute()) + ", " +
            safeString(config.getCheckTable()) + ", " +
            safeString(config.getCheckColumn()) +
            ");\n\n");
  }

  private RawSqlStatement insertMetadataSql(DdmRlsAddWriteRuleConfig config) {
    return new RawSqlStatement("INSERT INTO " + DdmConstants.METADATA_RLS_TABLE + "(" +
            DdmConstants.METADATA_FIELD_TYPE + ", " +
            DdmConstants.METADATA_FIELD_NAME + ", " +
            DdmConstants.METADATA_FIELD_JWT_ATTRIBUTE + ", " +
            DdmConstants.METADATA_FIELD_CHECK_TABLE + ", " +
            DdmConstants.METADATA_FIELD_CHECK_COLUMN +
            ") VALUES (" +
            safeString(DdmConstants.METADATA_TYPE_WRITE) + ", " +
            safeString(config.getName()) + ", " +
            safeString(config.getJwtAttribute()) + ", " +
            safeString(config.getCheckTable()) + ", " +
            safeString(config.getCheckColumn()) +
            ");\n\n");
  }

  private RawSqlStatement deleteMetadataSql(DdmRlsRemoveReadRuleConfig config) {
    return new RawSqlStatement("DELETE FROM " + DdmConstants.METADATA_RLS_TABLE +
            " WHERE " + DdmConstants.METADATA_FIELD_TYPE + "=" + safeString(DdmConstants.METADATA_TYPE_READ) +
            " AND " + DdmConstants.METADATA_FIELD_NAME + "=" + safeString(config.getName())
            );
  }

  private RawSqlStatement deleteMetadataSql(DdmRlsRemoveWriteRuleConfig config) {
    return new RawSqlStatement("DELETE FROM " + DdmConstants.METADATA_RLS_TABLE +
            " WHERE " + DdmConstants.METADATA_FIELD_TYPE + "=" + safeString(DdmConstants.METADATA_TYPE_WRITE) +
            " AND " + DdmConstants.METADATA_FIELD_NAME + "=" + safeString(config.getName())
    );
  }

  private String safeString(String data) {
    return (data != null && !data.isEmpty()) ? "'" + data + "'" : "NULL";
  }
}
