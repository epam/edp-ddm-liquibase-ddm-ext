package liquibase.statement.core;
import liquibase.change.DdmDomainConstraintConfig;
import liquibase.statement.AbstractSqlStatement;
import liquibase.statement.CompoundStatement;

import java.util.ArrayList;
import java.util.List;

public class DdmCreateDomainStatement extends AbstractSqlStatement implements CompoundStatement {
    private String name;
    private String type;
    private Boolean nullable;
    private String collation;
    private String defaultValue;
    private List<DdmDomainConstraintConfig> constraints;

    public DdmCreateDomainStatement(String name) {
        this.name = name;
        constraints = new ArrayList<>();
    }

    public DdmCreateDomainStatement(String name, String type) {
        this.name = name;
        this.type = type;
        constraints = new ArrayList<>();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Boolean getNullable() {
        return nullable;
    }

    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
    }

    public String getCollation() {
        return collation;
    }

    public void setCollation(String collation) {
        this.collation = collation;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public List<DdmDomainConstraintConfig> getConstraints() {
        return this.constraints;
    }

    public void setConstraints(List<DdmDomainConstraintConfig> constraints) {
        this.constraints = constraints;
    }

    public void addConstraint(DdmDomainConstraintConfig constraint) {
        this.constraints.add(constraint);
    }
}
