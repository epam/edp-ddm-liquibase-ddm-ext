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

package com.epam.digital.data.platform.liquibase.extension.change;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.serializer.AbstractLiquibaseSerializable;

public class DdmNestedEntityConfig extends AbstractLiquibaseSerializable {

  private String table;
  private String name;
  private List<DdmLinkConfig> linkConfigs;

  public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor)
      throws ParsedNodeException {
    List<DdmLinkConfig> loadedLinks = new ArrayList<>();
    for (ParsedNode child : parsedNode.getChildren()) {
      if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_TABLE)) {
        setTable(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_TABLE, String.class));
      }
      if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_NAME)) {
        setName(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_NAME, String.class));
      }
      if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_LINK)) {
        DdmLinkConfig linkConfig = new DdmLinkConfig();
        linkConfig.load(child, resourceAccessor);
        loadedLinks.add(linkConfig);
      }
    }
    setLinkConfig(loadedLinks);
  }

  @Override
  public String getSerializedObjectName() {
    return "ddmNestedEntity";
  }

  @Override
  public String getSerializedObjectNamespace() {
    return GENERIC_CHANGELOG_EXTENSION_NAMESPACE;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<DdmLinkConfig> getLinkConfig() {
    return linkConfigs;
  }

  public void setLinkConfig(List<DdmLinkConfig> linkConfigs) {
    this.linkConfigs = linkConfigs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DdmNestedEntityConfig that = (DdmNestedEntityConfig) o;
    boolean linksEqual = false;
    for (DdmLinkConfig linkConfig : linkConfigs) {
      linksEqual = that.linkConfigs.stream().anyMatch(linkConfig::equals);
    }
    return table.equals(that.table) && linksEqual;
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, linkConfigs);
  }
}
