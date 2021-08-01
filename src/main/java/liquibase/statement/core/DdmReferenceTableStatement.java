package liquibase.statement.core;

import liquibase.statement.AbstractSqlStatement;
import liquibase.statement.CompoundStatement;

public class DdmReferenceTableStatement extends AbstractSqlStatement implements CompoundStatement {
    private String tableName;

    public DdmReferenceTableStatement(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }
}
