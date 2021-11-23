package com.epam.digital.data.platform.liquibase.extension.change.core;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.DdmUtils;
import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.DatabaseChangeProperty;
import liquibase.database.Database;
import liquibase.statement.SqlStatement;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmDropTypeStatement;

/**
 * Drops an existing type.
 */
@DatabaseChange(name="dropType", description = "Drops an existing type", priority = ChangeMetaData.PRIORITY_DEFAULT, appliesTo = "type")
public class DdmDropTypeChange extends AbstractChange {

    private String name;

    @DatabaseChangeProperty(mustEqualExisting = "table", description = "Name of the table to drop")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        return new SqlStatement[]{ 
            new DdmDropTypeStatement(name),
            DdmUtils.deleteMetadataByChangeTypeAndChangeNameSql(
                DdmConstants.TYPE_METADATA_CHANGE_TYPE_VALUE, name), 
            DdmUtils.deleteMetadataByChangeTypeAndChangeNameSql(
                DdmConstants.TYPE_METADATA_ATTRIBUTE_NAME_LABEL, name)};
    }

    @Override
    public String getConfirmationMessage() {
        return "Type " + name + " dropped";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return STANDARD_CHANGELOG_NAMESPACE;
    }
}
