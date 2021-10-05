package com.epam.digital.data.platform.liquibase.extension.change.core;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.DdmUtils;
import com.epam.digital.data.platform.liquibase.extension.change.DdmLabelConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTypeConfig;
import liquibase.change.AbstractChange;
import liquibase.change.Change;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.DatabaseChangeProperty;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.statement.SqlStatement;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateTypeStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Creates a new Enum type.
 */
@DatabaseChange(name="createType", description = "Create Type", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class DdmCreateTypeChange extends AbstractChange {

    private String name;
    private DdmTypeConfig asComposite;
    private DdmTypeConfig asEnum;


    public DdmCreateTypeChange() {
        super();
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.addAll(super.validate(database));

        return validationErrors;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {

        DdmCreateTypeStatement statement = generateCreateTypeStatement();
        List<SqlStatement> statements = new ArrayList<>();
        statements.add(statement);

        if (asEnum != null) {
            statements.addAll(generateMetadataStatements(database));
        }

        return statements.toArray(new SqlStatement[statements.size()]);
    }

    protected DdmCreateTypeStatement generateCreateTypeStatement() {
        return new DdmCreateTypeStatement(getName(), getAsComposite(), getAsEnum());
    }

    private List<SqlStatement> generateMetadataStatements(Database database) {
        List<SqlStatement> statements = new ArrayList<>();

        for (DdmLabelConfig label : asEnum.getLabels()) {
            statements.add(DdmUtils.insertMetadataSql(DdmConstants.TYPE_METADATA_CHANGE_TYPE_VALUE, getName(), DdmConstants.TYPE_METADATA_ATTRIBUTE_NAME_LABEL, label.getLabel()));

            //  add translation
            statements.add(DdmUtils.insertMetadataSql(DdmConstants.TYPE_METADATA_ATTRIBUTE_NAME_LABEL, getName(), label.getLabel(), label.getTranslation()));
        }

        return statements;
    }

    @Override
    protected Change[] createInverses() {
        DdmDropTypeChange inverse = new DdmDropTypeChange();
        inverse.setName(getName());

        return new Change[]{
                inverse
        };
    }

    @DatabaseChangeProperty()
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getConfirmationMessage() {
        return "Type " + name + " created";
    }

    @DatabaseChangeProperty()
    public DdmTypeConfig getAsComposite() {
        return asComposite;
    }

    public void setAsComposite(DdmTypeConfig asComposite) {
        this.asComposite = asComposite;
    }

    @DatabaseChangeProperty()
    public DdmTypeConfig getAsEnum() {
        return asEnum;
    }

    public void setAsEnum(DdmTypeConfig asEnum) {
        this.asEnum = asEnum;
    }

}