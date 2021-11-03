package com.epam.digital.data.platform.liquibase.extension.statement.core;

import java.util.ArrayList;
import java.util.List;

import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import liquibase.statement.AbstractSqlStatement;

public class DdmPartialUpdateStatement extends AbstractSqlStatement {
    private List<DdmTableConfig> tables;
    private String name;

    public DdmPartialUpdateStatement(String name) {
        this.tables = new ArrayList<>();
        this.name = name;
    }

    public DdmPartialUpdateStatement() {
        this.tables = new ArrayList<>();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<DdmTableConfig> getTables() {
        return tables;
    }

    public void setTables(List<DdmTableConfig> tables) {
        this.tables = tables;
    }
}
