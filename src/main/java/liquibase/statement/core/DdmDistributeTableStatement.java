package liquibase.statement.core;

import liquibase.statement.AbstractSqlStatement;
import liquibase.statement.CompoundStatement;

public class DdmDistributeTableStatement extends AbstractSqlStatement implements CompoundStatement {
    private String tableName;
    private String distributionColumn;
    private String distributionType;
    private String colocateWith;

    public DdmDistributeTableStatement(String tableName, String distributionColumn) {
        this.tableName = tableName;
        this.distributionColumn = distributionColumn;
    }

    public DdmDistributeTableStatement(String tableName, String distributionColumn, String distributionType) {
        this.tableName = tableName;
        this.distributionColumn = distributionColumn;
        this.distributionType = distributionType;
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

    public DdmDistributeTableStatement setDistributionType(String distributionType) {
        this.distributionType = distributionType;
        return this;
    }

    public String getColocateWith() {
        return colocateWith;
    }

    public DdmDistributeTableStatement setColocateWith(String colocateWith) {
        this.colocateWith = colocateWith;
        return this;
    }
}
