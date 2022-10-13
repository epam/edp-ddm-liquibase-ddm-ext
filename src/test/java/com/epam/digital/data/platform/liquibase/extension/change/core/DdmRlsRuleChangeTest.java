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

package com.epam.digital.data.platform.liquibase.extension.change.core;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.change.DdmRlsAddReadRuleConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmRlsRemoveReadRuleConfig;
import liquibase.Contexts;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.core.MockDatabase;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.epam.digital.data.platform.liquibase.extension.DdmConstants.CONTEXT_SUB;

class DdmRlsRuleChangeTest {
  private DdmRlsRuleChange change;
  private ChangeLogParameters changeLogParameters;

  @BeforeEach
  void setUp() {
    DatabaseChangeLog changeLog = new DatabaseChangeLog("path");

    changeLogParameters = new ChangeLogParameters();
    changeLog.setChangeLogParameters(changeLogParameters);

    ChangeSet changeSet = new ChangeSet(changeLog);

    change = new DdmRlsRuleChange();
    change.setName("rls");

    changeSet.addChange(change);
    changeLog.addChangeSet(changeSet);

    DdmRlsAddReadRuleConfig config1 = new DdmRlsAddReadRuleConfig();
    config1.setJwtAttribute("KATOTTG");;
    config1.setName("rule1");
    config1.setCheckTable("table1");
    config1.setCheckColumn("col1");
    change.getAddReadRules().add(config1);

    DdmRlsAddReadRuleConfig config2 = new DdmRlsAddReadRuleConfig();
    config2.setJwtAttribute("KATOTTG");;
    config2.setName("rule2");
    config2.setCheckTable("table2");
    config2.setCheckColumn("col2");
    change.getAddReadRules().add(config2);

    DdmRlsRemoveReadRuleConfig config3 = new DdmRlsRemoveReadRuleConfig();
    config3.setName("rule1");
    change.getRemoveReadRules().add(config3);
  }

  @Test
  @DisplayName("Check statements")
  public void checkStatements() {
    setContext(CONTEXT_SUB);
    SqlStatement[] statements = change.generateStatements(new MockDatabase());
    Assertions.assertEquals(3, statements.length);
    Assertions.assertTrue(statements[0] instanceof RawSqlStatement);
    Assertions.assertEquals("DELETE FROM " + DdmConstants.METADATA_RLS_TABLE +
            " WHERE " + DdmConstants.METADATA_FIELD_TYPE + "='" + DdmConstants.METADATA_TYPE_READ +
            "' AND " + DdmConstants.METADATA_FIELD_NAME + "='rule1'" ,
            ((RawSqlStatement) statements[2]).getSql());
  }

  private void setContext(String ctx) {
    Contexts contexts = new Contexts();
    contexts.add(ctx);
    changeLogParameters.setContexts(contexts);
  }
}