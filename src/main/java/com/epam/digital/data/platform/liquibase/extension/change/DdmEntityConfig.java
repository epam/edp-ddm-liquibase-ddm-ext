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
import java.util.Objects;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.serializer.AbstractLiquibaseSerializable;

public class DdmEntityConfig extends AbstractLiquibaseSerializable {
  private String name;
  private String limit;

  public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor)
      throws ParsedNodeException {
      for(ParsedNode childItem : parsedNode.getChildren()) {
        if (childItem.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_NAME)) {
          setName(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_NAME, String.class));
        }
        if (childItem.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_LIMIT)) {
          setLimit(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_LIMIT, String.class));
        }
      }
  }

  @Override
  public String getSerializedObjectName() {
    return "ddmEntity";
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

  public String getLimit() {
    return limit;
  }

  public void setLimit(String limit) {
    this.limit = limit;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DdmEntityConfig that = (DdmEntityConfig) o;
    return name.equals(that.name) && limit.equals(that.limit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, limit);
  }
}
