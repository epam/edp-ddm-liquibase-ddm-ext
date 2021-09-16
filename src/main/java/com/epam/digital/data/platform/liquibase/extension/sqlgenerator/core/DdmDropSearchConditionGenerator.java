package com.epam.digital.data.platform.liquibase.extension.sqlgenerator.core;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmDropSearchConditionStatement;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

public class DdmDropSearchConditionGenerator extends AbstractSqlGenerator<DdmDropSearchConditionStatement> {

    @Override
    public ValidationErrors validate(DdmDropSearchConditionStatement ddmDropSearchConditionStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("name", ddmDropSearchConditionStatement.getName());
        return validationErrors;
    }

    @Override
    public Sql[] generateSql(DdmDropSearchConditionStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("drop view if exists ").append(statement.getName()).append(DdmConstants.SUFFIX_VIEW).append(";");
        buffer.append("\n\n");
        buffer.append("delete from ").append(DdmConstants.METADATA_TABLE);
        buffer.append(" where (").append(DdmConstants.METADATA_CHANGE_TYPE).append(" = '").append(DdmConstants.SEARCH_METADATA_CHANGE_TYPE_VALUE).append("') and (");
        buffer.append(DdmConstants.METADATA_CHANGE_NAME).append(" = '").append(statement.getName()).append("');");
        buffer.append("\n\n");
        buffer.append("delete from ").append(DdmConstants.METADATA_TABLE);
        buffer.append(" where (").append(DdmConstants.METADATA_CHANGE_TYPE).append(" = '").append(statement.getName()).append("');");
        buffer.append("\n\n");
        buffer.append("delete from ").append(DdmConstants.METADATA_TABLE);
        buffer.append(" where (").append(DdmConstants.METADATA_ATTRIBUTE_NAME).append(" = '").append(DdmConstants.SEARCH_METADATA_CHANGE_TYPE_VALUE).append("') and (");
        buffer.append(DdmConstants.METADATA_ATTRIBUTE_VALUE).append(" = '").append(statement.getName()).append("');");

        return new Sql[]{
            new UnparsedSql(buffer.toString())
        };
    }

 }
