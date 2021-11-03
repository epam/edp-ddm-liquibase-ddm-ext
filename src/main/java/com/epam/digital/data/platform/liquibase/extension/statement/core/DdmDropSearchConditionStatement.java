package com.epam.digital.data.platform.liquibase.extension.statement.core;

import liquibase.statement.AbstractSqlStatement;

public class DdmDropSearchConditionStatement extends AbstractSqlStatement {

    private final String name;

    public DdmDropSearchConditionStatement(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
