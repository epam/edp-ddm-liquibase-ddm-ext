package liquibase.statement.core;

import liquibase.statement.AbstractSqlStatement;

public class DdmDropTypeStatement extends AbstractSqlStatement {

    private String name;

    public DdmDropTypeStatement(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
