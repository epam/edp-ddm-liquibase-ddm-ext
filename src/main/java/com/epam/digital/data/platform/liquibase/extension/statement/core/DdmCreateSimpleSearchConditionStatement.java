package com.epam.digital.data.platform.liquibase.extension.statement.core;

import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import liquibase.statement.AbstractSqlStatement;
import liquibase.statement.CompoundStatement;

public class DdmCreateSimpleSearchConditionStatement extends AbstractSqlStatement implements CompoundStatement {

    private String name;
    private DdmTableConfig table;
    private DdmColumnConfig searchColumn;
    private boolean indexing;
    private String limit;

    public DdmCreateSimpleSearchConditionStatement(String name) {
        super();
        this.name = name;
    }

    public DdmTableConfig getTable() {
        return this.table;
    }

    public void setTable(DdmTableConfig table) {
        this.table = table;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean getIndexing() {
        return indexing;
    }

    public void setIndexing(boolean indexing) {
        this.indexing = indexing;
    }

    public DdmColumnConfig getSearchColumn() {
        return searchColumn;
    }

    public void setSearchColumn(DdmColumnConfig searchColumn) {
        this.searchColumn = searchColumn;
    }

    public String getLimit() {
        return limit;
    }

    public void setLimit(String limit) {
        this.limit = limit;
    }
}
