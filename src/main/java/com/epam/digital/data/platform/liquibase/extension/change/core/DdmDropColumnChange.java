package com.epam.digital.data.platform.liquibase.extension.change.core;

import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.core.DropColumnChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;

import java.util.Objects;


/**
 * Drops an existing column from a table.
 */

@DatabaseChange(
    name = "dropColumn",
    description = "Drop existing column(s).\n" +
        "\n" +
        "To drop a single column, use the simple form of this element where the tableName and " +
        "columnName are specified as attributes. To drop several columns, specify the tableName " +
        "as an attribute, and then specify a set of nested <column> tags. If nested <column> tags " +
        "are present, the columnName attribute will be ignored.",
    priority = ChangeMetaData.PRIORITY_DEFAULT + 50,
    appliesTo = "column")
public class DdmDropColumnChange extends DropColumnChange {

    private Boolean historyFlag;

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();

        if (Objects.nonNull(getHistoryFlag()) && getHistoryFlag()) {
            validationErrors.addError("'dropColumn' is not allowed");
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
