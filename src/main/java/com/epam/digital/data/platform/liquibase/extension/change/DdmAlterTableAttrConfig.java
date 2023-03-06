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

package com.epam.digital.data.platform.liquibase.extension.change;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.serializer.AbstractLiquibaseSerializable;

/**
 * Creates a new attribute.
 */
public class DdmAlterTableAttrConfig extends AbstractLiquibaseSerializable {

  private String name;
  private String value;

  @Override
  public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
    setName(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_NAME, String.class));
    setValue(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_VALUE, String.class));
  }

  @Override
  public String getSerializedObjectName() {
    return "ddmAlterTableAttribute";
  }

  @Override
  public String getSerializedObjectNamespace() {
    return GENERIC_CHANGELOG_EXTENSION_NAMESPACE;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }
}