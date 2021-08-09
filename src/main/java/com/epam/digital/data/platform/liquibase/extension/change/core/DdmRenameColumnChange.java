package com.epam.digital.data.platform.liquibase.extension.change.core;

import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.core.RenameColumnChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;

import java.util.Objects;

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

        if (Objects.nonNull(getHistoryFlag()) && getHistoryFlag()) {
            validationErrors.addError("'renameColumn' is not allowed");
        } else {
            validationErrors.addAll(super.validate(database));
        }

        return validationErrors;
    }

    public Boolean getHistoryFlag() {
        return historyFlag;
    }

    public void setHistoryFlag(Boolean historyFlag) {
        this.historyFlag = historyFlag;
    }
}
