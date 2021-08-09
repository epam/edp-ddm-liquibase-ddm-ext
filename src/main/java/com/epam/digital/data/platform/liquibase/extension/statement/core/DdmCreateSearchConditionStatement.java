package com.epam.digital.data.platform.liquibase.extension.statement.core;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.change.DdmConditionConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmCteConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmJoinConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import liquibase.statement.AbstractSqlStatement;
import liquibase.statement.CompoundStatement;

import java.util.ArrayList;
import java.util.List;

public class DdmCreateSearchConditionStatement extends AbstractSqlStatement implements CompoundStatement {

    private List<DdmCteConfig> ctes;
    private List<DdmTableConfig> tables;
    private List<DdmJoinConfig> joins;
    private String name;
    private Boolean indexing;
    private String limit;
    private List<DdmConditionConfig> conditions;

    public DdmCreateSearchConditionStatement(String name) {
        super();
        this.name = name;
        this.ctes = new ArrayList<>();
        this.tables = new ArrayList<>();
        this.joins = new ArrayList<>();
    }

    public String getViewName() {
        return this.name + DdmConstants.SUFFIX_VIEW;
    }

    public List<DdmCteConfig> getCtes() {
        return ctes;
    }

    public void setCtes(List<DdmCteConfig> ctes) {
        this.ctes = ctes;
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
}
