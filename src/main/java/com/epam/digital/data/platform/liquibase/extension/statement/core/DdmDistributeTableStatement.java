package com.epam.digital.data.platform.liquibase.extension.statement.core;

import liquibase.statement.AbstractSqlStatement;
import liquibase.statement.CompoundStatement;

public class DdmDistributeTableStatement extends AbstractSqlStatement implements CompoundStatement {
    private final String tableName;
    private final String distributionColumn;
    private String distributionType;
    private String colocateWith;

    public DdmDistributeTableStatement(String tableName, String distributionColumn) {
        this.tableName = tableName;
        this.distributionColumn = distributionColumn;
    }

    public String getTableName() {
        return tableName;
    }

    public String getDistributionColumn() {
        return distributionColumn;
    }

    public String getDistributionType() {
        return distributionType;
    }

    public void setDistributionType(String distributionType) {
        this.distributionType = distributionType;
    }

    public String getColocateWith() {
        return colocateWith;
    }

    public void setColocateWith(String colocateWith) {
        this.colocateWith = colocateWith;
    }
}
