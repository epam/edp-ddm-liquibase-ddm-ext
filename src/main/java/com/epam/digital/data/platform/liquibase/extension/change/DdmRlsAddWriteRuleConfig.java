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

package com.epam.digital.data.platform.liquibase.extension.change;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.serializer.AbstractLiquibaseSerializable;

public class DdmRlsAddWriteRuleConfig extends AbstractLiquibaseSerializable {

  private String name;
  private String jwtAttribute;
  private String checkColumn;
  private String checkTable;

  @Override
  public String getSerializedObjectName() {
    return "addWriteRule";
  }

  @Override
  public String getSerializedObjectNamespace() {
    return GENERIC_CHANGELOG_EXTENSION_NAMESPACE;
  }

  @Override
  public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
    setName(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_NAME, String.class));
    setJwtAttribute(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_JWT_ATTRIBUTE, String.class));
    setCheckColumn(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_CHECK_COLUMN, String.class));
    setCheckTable(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_CHECK_TABLE, String.class));

  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getJwtAttribute() {
    return jwtAttribute;
  }

  public void setJwtAttribute(String jwtAttribute) {
    this.jwtAttribute = jwtAttribute;
  }

  public String getCheckColumn() {
    return checkColumn;
  }

  public void setCheckColumn(String checkColumn) {
    this.checkColumn = checkColumn;
  }

  public String getCheckTable() {
    return checkTable;
  }

  public void setCheckTable(String checkTable) {
    this.checkTable = checkTable;
  }

}
