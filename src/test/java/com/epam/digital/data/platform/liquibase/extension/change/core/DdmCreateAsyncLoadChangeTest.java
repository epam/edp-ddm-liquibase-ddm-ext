/*
 * Copyright 2023 EPAM Systems.
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

import com.epam.digital.data.platform.liquibase.extension.DdmTest;
import com.epam.digital.data.platform.liquibase.extension.change.DdmEntityConfig;
import java.util.ArrayList;
import java.util.List;
import liquibase.change.Change;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.core.MockDatabase;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DdmCreateAsyncLoadChangeTest {

  private DdmCreateAsyncLoadChange change;

  @BeforeEach
  public void setUp() {
    DatabaseChangeLog changeLog = new DatabaseChangeLog("path");
    ChangeSet changeSet = new ChangeSet(changeLog);

    change = new DdmCreateAsyncLoadChange();

    changeSet.addChange(change);

    changeLog.addChangeSet(changeSet);
  }

  @Test
  public void checkStatements() {
    change.setName("name");
    change.setEntityList(createEntities());
    SqlStatement[] statements = change.generateStatements(new MockDatabase());

    Assertions.assertEquals(2, statements.length);
    Assertions.assertTrue(statements[0] instanceof RawSqlStatement);
    Assertions.assertTrue(statements[1] instanceof RawSqlStatement);
    Assertions.assertEquals(
        "insert into ddm_liquibase_metadata(change_type, change_name, attribute_name, attribute_value) values ('create_async_load', 'name', 'testEntityName1', '100');\n\n",
        ((RawSqlStatement) statements[0]).getSql());
    Assertions.assertEquals(
        "insert into ddm_liquibase_metadata(change_type, change_name, attribute_name, attribute_value) values ('create_async_load', 'name', 'testEntityName2', '200');\n\n",
        ((RawSqlStatement) statements[1]).getSql());
  }

  @Test
  public void checkLoad() throws Exception {
    List<ChangeSet> changeSets = DdmTest.loadChangeSets(
        DdmTest.TEST_CREATE_ASYNC_LOAD_FILE_NAME);
    Change change = changeSets.get(0).getChanges().get(0);

    Assertions.assertEquals(1, changeSets.size());
    Assertions.assertEquals(2, changeSets.get(0).getChanges().size());
    Assertions.assertTrue(change instanceof DdmCreateAsyncLoadChange);

    Assertions.assertEquals("composite_nested_entities", changeSets.get(0).getId());

    DdmCreateAsyncLoadChange change1 =
        (DdmCreateAsyncLoadChange) changeSets.get(0).getChanges().get(0);
    Assertions.assertEquals("allowedAsyncLoads", change1.getName());
    Assertions.assertEquals(2, change1.getEntityList().size());
    DdmEntityConfig dec = change1.getEntityList().get(0);
    Assertions.assertEquals("item", dec.getName());
    Assertions.assertEquals("100", dec.getLimit());
    dec = change1.getEntityList().get(1);
    Assertions.assertEquals("item_with_references", dec.getName());
    Assertions.assertEquals("1000", dec.getLimit());

    DdmCreateAsyncLoadChange change2 =
        (DdmCreateAsyncLoadChange) changeSets.get(0).getChanges().get(1);
    Assertions.assertEquals("allowedAsyncLoads2", change2.getName());
    Assertions.assertEquals(3, change2.getEntityList().size());
    dec = change2.getEntityList().get(0);
    Assertions.assertEquals("item2", dec.getName());
    Assertions.assertEquals("20", dec.getLimit());
    dec = change2.getEntityList().get(1);
    Assertions.assertEquals("item_with_references2", dec.getName());
    Assertions.assertEquals("200", dec.getLimit());
    dec = change2.getEntityList().get(2);
    Assertions.assertEquals("demo_entity2", dec.getName());
    Assertions.assertEquals("2000", dec.getLimit());

  }

  private List<DdmEntityConfig> createEntities() {
    List<DdmEntityConfig> entities = new ArrayList<>();

    DdmEntityConfig entity = new DdmEntityConfig();
    entity.setName("testEntityName1");
    entity.setLimit("100");

    DdmEntityConfig entity2 = new DdmEntityConfig();
    entity2.setName("testEntityName2");
    entity2.setLimit("200");

    entities.add(entity);
    entities.add(entity2);

    return entities;
  }

}