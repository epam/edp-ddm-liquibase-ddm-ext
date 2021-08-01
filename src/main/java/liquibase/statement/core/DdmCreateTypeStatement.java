package liquibase.statement.core;

import liquibase.change.DdmTypeConfig;
import liquibase.statement.AbstractSqlStatement;
import liquibase.statement.CompoundStatement;

public class DdmCreateTypeStatement extends AbstractSqlStatement implements CompoundStatement {
    private String name;
    private DdmTypeConfig asComposite;
    private DdmTypeConfig asEnum;

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
