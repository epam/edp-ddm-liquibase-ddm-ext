package com.epam.digital.data.platform.liquibase.extension.change.core;

import com.epam.digital.data.platform.liquibase.extension.change.DdmDomainConstraintConfig;
import liquibase.change.AbstractChange;
import liquibase.change.Change;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.DatabaseChangeProperty;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.statement.SqlStatement;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateDomainStatement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Creates a new Domain.
 */
@DatabaseChange(name="createDomain", description = "Create Domain", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class DdmCreateDomainChange extends AbstractChange {

    private String name;
    private String type;
    private Boolean nullable;
    private String collation;
    private String defaultValue;
    private List<DdmDomainConstraintConfig> constraints;

    public DdmCreateDomainChange() {
        super();
        constraints = new ArrayList<>();
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.addAll(super.validate(database));
        return validationErrors;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        DdmCreateDomainStatement statement = generateDdmCreateDomainStatement();
        statement.setNullable(getNullable());
        statement.setCollation(getCollation());
        statement.setDefaultValue(getDefaultValue());
        statement.setConstraints(getConstraints());

        List<SqlStatement> statements = new ArrayList<>();
        statements.add(statement);

        return statements.toArray(new SqlStatement[0]);
    }

    protected DdmCreateDomainStatement generateDdmCreateDomainStatement() {
        return new DdmCreateDomainStatement(getName(), getType());
    }

    @Override
    protected Change[] createInverses() {
        DdmDropDomainChange inverse = new DdmDropDomainChange();
        inverse.setName(getName());
        return new Change[]{ inverse };
    }

    @DatabaseChangeProperty()
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @DatabaseChangeProperty()
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @DatabaseChangeProperty()
    public Boolean getNullable() {
        return nullable;
    }

    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
    }

    @DatabaseChangeProperty()
    public String getCollation() {
        return collation;
    }

    public void setCollation(String collation) {
        this.collation = collation;
    }

    @DatabaseChangeProperty()
    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    @DatabaseChangeProperty()
    public List<DdmDomainConstraintConfig> getConstraints() {
        return constraints == null ? Collections.emptyList() : this.constraints;
    }

    public void setConstraints(List<DdmDomainConstraintConfig> constraints) {
        this.constraints = constraints;
    }

    public void addConstraint(DdmDomainConstraintConfig constraint) {
        this.constraints.add(constraint);
    }

    @Override
    public String getConfirmationMessage() {
        return "Domain " + this.name + " created";
    }

    @Override
    public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
        super.load(parsedNode, resourceAccessor);

        for (ParsedNode child : parsedNode.getChildren()) {
            if ("constraint".equals(child.getName())) {
                String cName = child.getChildValue(null, "name", String.class);
                String cImplementation = child.getChildValue(null, "implementation", String.class);
                addConstraint(new DdmDomainConstraintConfig(cName, cImplementation));
            }
        }

    }
}