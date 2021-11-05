package com.epam.digital.data.platform.liquibase.extension.change.core;

import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.core.RenameColumnChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;

/**
 * Renames an existing column.
 */

@DatabaseChange(
    name="renameColumn",
    description = "Renames an existing column",
    priority = ChangeMetaData.PRIORITY_DEFAULT + 50,
    appliesTo = "column"
)
public class DdmRenameColumnChange extends RenameColumnChange {

    private Boolean historyFlag;

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();
        return Boolean.TRUE.equals(historyFlag) ?
            validationErrors.addError("'renameColumn' is not allowed") :
            validationErrors.addAll(super.validate(database));
    }

    public Boolean getHistoryFlag() {
        return historyFlag;
    }

    public void setHistoryFlag(Boolean historyFlag) {
        this.historyFlag = historyFlag;
    }
}
