package com.epam.digital.data.platform.liquibase.extension.change;

import liquibase.serializer.AbstractLiquibaseSerializable;

public class DdmLabelConfig extends AbstractLiquibaseSerializable {
    private String label;
    private String translation;

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getTranslation() {
        return translation;
    }

    public void setTranslation(String translation) {
        this.translation = translation;
    }

    @Override
    public String getSerializedObjectName() {
        return "ddmLabel";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return GENERIC_CHANGELOG_EXTENSION_NAMESPACE;
    }
}
