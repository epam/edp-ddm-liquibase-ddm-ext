package liquibase.statement.core;

import liquibase.statement.AbstractSqlStatement;

public class DdmDropSearchConditionStatement extends AbstractSqlStatement {

    private String name;

    public DdmDropSearchConditionStatement(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
