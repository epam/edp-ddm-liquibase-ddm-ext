package com.epam.digital.data.platform.liquibase.extension.change.core;

import liquibase.change.AbstractChange;
import liquibase.change.DatabaseChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChangeProperty;
import liquibase.database.Database;
import liquibase.statement.SqlStatement;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmDropSearchConditionStatement;

/**
 * Drop search condition.
 */
@DatabaseChange(name="dropSearchCondition", description = "Drop Search Condition", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class DdmDropSearchConditionChange extends AbstractChange {

    private String name;

    @Override
    public SqlStatement[] generateStatements(Database database) {
        return new SqlStatement[]{ new DdmDropSearchConditionStatement(getName()) };
    }

    @Override
    public String getConfirmationMessage() {
        return "Search Condition " + getName() + " dropped";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return STANDARD_CHANGELOG_NAMESPACE;
    }

    @DatabaseChangeProperty(description = "Name of the Search Condition to drop")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
