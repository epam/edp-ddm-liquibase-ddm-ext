package com.epam.digital.data.platform.liquibase.extension.statement.core;

import liquibase.statement.AbstractSqlStatement;

public class DdmDropTypeStatement extends AbstractSqlStatement {

    private final String name;

    public DdmDropTypeStatement(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
