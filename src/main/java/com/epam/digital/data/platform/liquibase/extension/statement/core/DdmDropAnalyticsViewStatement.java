package com.epam.digital.data.platform.liquibase.extension.statement.core;

import liquibase.statement.AbstractSqlStatement;

public class DdmDropAnalyticsViewStatement extends AbstractSqlStatement {

    private final String name;

    public DdmDropAnalyticsViewStatement(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
