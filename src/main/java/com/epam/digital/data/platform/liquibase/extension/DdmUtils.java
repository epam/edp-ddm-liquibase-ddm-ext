package com.epam.digital.data.platform.liquibase.extension;

import liquibase.change.ColumnConfig;
import liquibase.changelog.ChangeSet;
import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.statement.core.InsertStatement;

public class DdmUtils {

    public static InsertStatement insertMetadata(String change_type, String change_name, String attributeName, String attributeValue) {
        InsertStatement insertStatement = new InsertStatement(null, null, DdmConstants.METADATA_TABLE);
        insertStatement.addColumn(new ColumnConfig().setName(DdmConstants.METADATA_CHANGE_TYPE).setValue(change_type));
        insertStatement.addColumn(new ColumnConfig().setName(DdmConstants.METADATA_CHANGE_NAME).setValue(change_name));
        insertStatement.addColumn(new ColumnConfig().setName(DdmConstants.METADATA_ATTRIBUTE_NAME).setValue(attributeName));
        insertStatement.addColumn(new ColumnConfig().setName(DdmConstants.METADATA_ATTRIBUTE_VALUE).setValue(attributeValue));

        return insertStatement;
    }

    public static String insertMetadataSql(String change_type, String change_name, String attributeName, String attributeValue, Database database) {
        return "insert into " + database.escapeTableName(null, null, DdmConstants.METADATA_TABLE) + "(" +
            database.escapeColumnName(null, null, DdmConstants.METADATA_TABLE, DdmConstants.METADATA_CHANGE_TYPE) + ", " +
            database.escapeColumnName(null, null, DdmConstants.METADATA_TABLE, DdmConstants.METADATA_CHANGE_NAME) + ", " +
            database.escapeColumnName(null, null, DdmConstants.METADATA_TABLE, DdmConstants.METADATA_ATTRIBUTE_NAME) + ", " +
            database.escapeColumnName(null, null, DdmConstants.METADATA_TABLE, DdmConstants.METADATA_ATTRIBUTE_VALUE) +
            ") values (" +
            DataTypeFactory
                .getInstance().fromObject(change_type, database).objectToSql(change_type, database) + ", " +
            DataTypeFactory.getInstance().fromObject(change_name, database).objectToSql(change_name, database) + ", " +
            DataTypeFactory.getInstance().fromObject(attributeName, database).objectToSql(attributeName, database) + ", " +
            DataTypeFactory.getInstance().fromObject(attributeValue, database).objectToSql(attributeValue, database) + ");\n\n";
    }

    public static boolean hasContext(ChangeSet changeSet, String context) {
        return Boolean.TRUE.equals(changeSet.getChangeLog().getChangeLogParameters().getContexts().getContexts().contains(context));
    }
}
