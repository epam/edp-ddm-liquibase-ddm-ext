package com.epam.digital.data.platform.liquibase.extension.statement.core;

import com.epam.digital.data.platform.liquibase.extension.change.DdmTypeConfig;
import liquibase.statement.AbstractSqlStatement;
import liquibase.statement.CompoundStatement;

public class DdmCreateTypeStatement extends AbstractSqlStatement implements CompoundStatement {
    private final String name;
    private final DdmTypeConfig asComposite;
    private final DdmTypeConfig asEnum;

    public DdmCreateTypeStatement(String name, DdmTypeConfig asComposite, DdmTypeConfig asEnum) {
        this.name = name;
        this.asComposite = asComposite;
        this.asEnum = asEnum;
    }

    public String getName() {
        return name;
    }

    public DdmTypeConfig getAsComposite() {
        return asComposite;
    }

    public DdmTypeConfig getAsEnum() {
        return asEnum;
    }
}
