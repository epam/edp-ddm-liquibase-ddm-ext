package com.epam.digital.data.platform.liquibase.extension.change;

import liquibase.change.DatabaseChangeProperty;
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

    public void setName(String name) {
        this.name = name;
    }

    @DatabaseChangeProperty(requiredForDatabase = "all")
    public String getImplementation() {
        return implementation;
    }

    public void setImplementation(String implementation) {
        this.implementation = implementation;
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
