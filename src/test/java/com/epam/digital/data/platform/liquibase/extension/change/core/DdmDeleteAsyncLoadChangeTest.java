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
import com.epam.digital.data.platform.liquibase.extension.change.DdmDeleteEntityConfig;
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

class DdmDeleteAsyncLoadChangeTest {

  private DdmDeleteAsyncLoadChange change;

  @BeforeEach
  public void setUp() {
    DatabaseChangeLog changeLog = new DatabaseChangeLog("path");
    ChangeSet changeSet = new ChangeSet(changeLog);

    change = new DdmDeleteAsyncLoadChange();

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
        "delete from ddm_liquibase_metadata where attribute_name = 'testDeleteEntityName1';\n\n",
        ((RawSqlStatement) statements[0]).getSql());
    Assertions.assertEquals(
        "delete from ddm_liquibase_metadata where attribute_name = 'testDeleteEntityName2';\n\n",
        ((RawSqlStatement) statements[1]).getSql());
  }

  @Test
  public void checkLoad() throws Exception {
    List<ChangeSet> changeSets = DdmTest.loadChangeSets(
        DdmTest.TEST_DELETE_ASYNC_LOAD_FILE_NAME);
    Change change = changeSets.get(0).getChanges().get(0);

    Assertions.assertEquals(1, changeSets.size());
    Assertions.assertTrue(change instanceof DdmDeleteAsyncLoadChange);
    Assertions.assertEquals(2, ((DdmDeleteAsyncLoadChange) change).getEntityList().size());

    Assertions.assertEquals("remove_async_entities", changeSets.get(0).getId());

    DdmDeleteAsyncLoadChange deleteChange =
        (DdmDeleteAsyncLoadChange) changeSets.get(0).getChanges().get(0);
    Assertions.assertEquals("deletedAsyncLoads", deleteChange.getName());
    DdmDeleteEntityConfig dec = deleteChange.getEntityList().get(0);
    Assertions.assertEquals("item31", dec.getName());

    dec = deleteChange.getEntityList().get(1);
    Assertions.assertEquals("demo_entity31", dec.getName());

  }

  private List<DdmDeleteEntityConfig> createEntities() {
    List<DdmDeleteEntityConfig> entities = new ArrayList<>();

    DdmDeleteEntityConfig entity = new DdmDeleteEntityConfig();
    entity.setName("testDeleteEntityName1");

    DdmDeleteEntityConfig entity2 = new DdmDeleteEntityConfig();
    entity2.setName("testDeleteEntityName2");

    entities.add(entity);
    entities.add(entity2);

    return entities;
  }
}