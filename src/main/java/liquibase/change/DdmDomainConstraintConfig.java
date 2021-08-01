package liquibase.change;

import liquibase.serializer.AbstractLiquibaseSerializable;

public class DdmDomainConstraintConfig extends AbstractLiquibaseSerializable {
    private String name;
    private String implementation;

    public DdmDomainConstraintConfig() {
    }

    public DdmDomainConstraintConfig(String name, String implementation) {
        this.name = name;
        this.implementation = implementation;
    }

    public String getName() {
        return name;
    }

    public DdmDomainConstraintConfig setName(String name) {
        this.name = name;
        return this;
    }

    @DatabaseChangeProperty(requiredForDatabase = "all")
    public String getImplementation() {
        return implementation;
    }

    public DdmDomainConstraintConfig setImplementation(String implementation) {
        this.implementation = implementation;
        return this;
    }

    @Override
    public String getSerializedObjectName() {
        return "ddmDomainConstraint";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return GENERIC_CHANGELOG_EXTENSION_NAMESPACE;
    }
}
