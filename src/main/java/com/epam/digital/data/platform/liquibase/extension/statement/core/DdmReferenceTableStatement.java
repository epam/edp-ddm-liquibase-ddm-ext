package com.epam.digital.data.platform.liquibase.extension.statement.core;

import liquibase.statement.AbstractSqlStatement;
import liquibase.statement.CompoundStatement;

public class DdmReferenceTableStatement extends AbstractSqlStatement implements CompoundStatement {
    private final String tableName;

    public DdmReferenceTableStatement(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }
}
