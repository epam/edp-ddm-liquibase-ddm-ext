package com.epam.digital.data.platform.liquibase.extension.change;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.serializer.AbstractLiquibaseSerializable;

public class DdmSearchConditionConfig extends AbstractLiquibaseSerializable {

  private String name;
  private String alias;


  public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
    setName(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_NAME, String.class));
    setAlias(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_ALIAS, String.class));
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }


  @Override
  public String getSerializedObjectName() {
    return "ddmSearchCondition";
  }

  @Override
  public String getSerializedObjectNamespace() {
    return GENERIC_CHANGELOG_EXTENSION_NAMESPACE;
  }

}
