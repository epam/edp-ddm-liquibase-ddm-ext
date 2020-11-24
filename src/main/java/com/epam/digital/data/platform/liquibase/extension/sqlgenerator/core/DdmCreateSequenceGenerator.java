/*
 * Copyright 2021 EPAM Systems.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.epam.digital.data.platform.liquibase.extension.sqlgenerator.core;

import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateSequenceStatement;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

public class DdmCreateSequenceGenerator extends AbstractSqlGenerator<DdmCreateSequenceStatement> {

  private static final String TEMPLATE = "CREATE SEQUENCE IF NOT EXISTS %s_%s_seq INCREMENT BY 1 OWNED BY %s.%s;";

  @Override
  public ValidationErrors validate(DdmCreateSequenceStatement statement, Database database,
      SqlGeneratorChain sqlGeneratorChain) {
    return new ValidationErrors();
  }

  @Override
  public Sql[] generateSql(DdmCreateSequenceStatement statement, Database database,
      SqlGeneratorChain sqlGeneratorChain) {
    String sql = String.format(TEMPLATE, statement.getTableName(), statement.getColumnName(),
        statement.getTableName(), statement.getColumnName());

    return new Sql[]{new UnparsedSql(sql)};
  }
}
