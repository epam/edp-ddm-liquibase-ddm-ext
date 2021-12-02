package com.epam.digital.data.platform.liquibase.extension.sqlgenerator.core;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmDropAnalyticsViewStatement;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

public class DdmDropAnalyticsViewGenerator extends AbstractSqlGenerator<DdmDropAnalyticsViewStatement> {

    @Override
    public ValidationErrors validate(DdmDropAnalyticsViewStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("name", statement.getName());
        return validationErrors;
    }

    @Override
    public Sql[] generateSql(DdmDropAnalyticsViewStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        return new Sql[]{ new UnparsedSql("drop view if exists " +
            statement.getName() + DdmConstants.SUFFIX_VIEW + ";") };
    }

 }
