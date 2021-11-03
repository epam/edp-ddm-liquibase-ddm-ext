package com.epam.digital.data.platform.liquibase.extension.statement.core;

import liquibase.statement.AbstractSqlStatement;

public class DdmDropDomainStatement extends AbstractSqlStatement {

    private final String name;

    public DdmDropDomainStatement(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
