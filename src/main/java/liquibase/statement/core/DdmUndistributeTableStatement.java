package liquibase.statement.core;

import liquibase.statement.AbstractSqlStatement;
import liquibase.statement.CompoundStatement;

public class DdmUndistributeTableStatement extends AbstractSqlStatement implements CompoundStatement {
    private String tableName;

    public DdmUndistributeTableStatement(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }
}
