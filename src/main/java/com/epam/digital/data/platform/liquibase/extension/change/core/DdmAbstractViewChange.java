package com.epam.digital.data.platform.liquibase.extension.change.core;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.change.DdmConditionConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmCteConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmFunctionConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmJoinConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateAbstractViewStatement;
import liquibase.change.AbstractChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;

import java.util.ArrayList;
import java.util.List;
import liquibase.util.StringUtil;

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
  private Boolean pagination;
  private List<DdmConditionConfig> conditions;

  public DdmAbstractViewChange() {
    super();
    this.ctes = new ArrayList<>();
    this.tables = new ArrayList<>();
    this.joins = new ArrayList<>();
  }

  public DdmAbstractViewChange(String name) {
    this();
    this.name = name;
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
        if (function.getName().equals(DdmConstants.ATTRIBUTE_FUNCTION_STRING_AGG) &&
            StringUtil.isEmpty(function.getParameter())) {
          validationErrors.addError("function " + function.getName().toUpperCase() + " required additional parameter!");
        } else if (function.hasParameter()) {
          validationErrors.addError("function " + function.getName().toUpperCase() + " doesn't required additional parameter!");
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
    this.name = name;
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

  public Boolean getPagination() {
    return pagination;
  }

  public void setPagination(Boolean pagination) {
    this.pagination = pagination;
  }
}
