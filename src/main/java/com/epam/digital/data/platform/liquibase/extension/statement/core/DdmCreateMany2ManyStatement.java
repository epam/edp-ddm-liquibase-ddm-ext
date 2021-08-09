package com.epam.digital.data.platform.liquibase.extension.statement.core;

import java.util.ArrayList;
import java.util.List;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import liquibase.statement.AbstractSqlStatement;
import liquibase.statement.CompoundStatement;

public class DdmCreateMany2ManyStatement extends AbstractSqlStatement implements CompoundStatement {

    private String mainTableName;
    private String mainTableKeyField;
    private String referenceTableName;
    private String referenceKeysArray;
    private List<DdmColumnConfig> mainTableColumns = new ArrayList<>();
    private List<DdmColumnConfig> referenceTableColumns = new ArrayList<>();


    public DdmCreateMany2ManyStatement() {
        super();
    }

    public String getName() {
        return mainTableName + "_" + referenceTableName;
    }

    public String getRelationName() {
        return getName() + DdmConstants.SUFFIX_RELATION;
    }

    public String getViewName() {
        return getRelationName() + DdmConstants.SUFFIX_VIEW;
    }

    public String getMainTableName() {
        return mainTableName;
    }

    public void setMainTableName(String mainTableName) {
        this.mainTableName = mainTableName;
    }

    public String getMainTableKeyField() {
        return mainTableKeyField;
    }

    public void setMainTableKeyField(String mainTableKeyField) {
        this.mainTableKeyField = mainTableKeyField;
    }

    public String getReferenceTableName() {
        return referenceTableName;
    }

    public void setReferenceTableName(String referenceTableName) {
        this.referenceTableName = referenceTableName;
    }

    public String getReferenceKeysArray() {
        return referenceKeysArray;
    }

    public void setReferenceKeysArray(String referenceKeysArray) {
        this.referenceKeysArray = referenceKeysArray;
    }

    public List<DdmColumnConfig> getMainTableColumns() {
        return mainTableColumns;
    }

    public void setMainTableColumns(List<DdmColumnConfig> mainTableColumns) {
        this.mainTableColumns = mainTableColumns;
    }

    public List<DdmColumnConfig> getReferenceTableColumns() {
        return referenceTableColumns;
    }

    public void setReferenceTableColumns(
        List<DdmColumnConfig> referenceTableColumns) {
        this.referenceTableColumns = referenceTableColumns;
    }
}
