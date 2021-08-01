package liquibase.statement.core;

import liquibase.statement.AbstractSqlStatement;

public class DdmDropDomainStatement extends AbstractSqlStatement {

    private String name;

    public DdmDropDomainStatement(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
