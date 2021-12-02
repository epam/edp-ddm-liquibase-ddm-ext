package com.epam.digital.data.platform.liquibase.extension.change.core;

import com.epam.digital.data.platform.liquibase.extension.DdmUtils;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.core.CreateIndexChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.statement.SqlStatement;

@DatabaseChange(name="createAnalyticsIndex", description = "Create Analytics Index",
    priority = ChangeMetaData.PRIORITY_DEFAULT)
public class DdmCreateAnalyticsIndexChange extends CreateIndexChange {

  public DdmCreateAnalyticsIndexChange () {
    super();
  }

  @Override
  public ValidationErrors validate(Database database) {
    ValidationErrors validationErrors = new ValidationErrors();
    validationErrors.addAll(super.validate(database));
    if (!DdmUtils.isAnalyticsChangeSet(this.getChangeSet())){
      validationErrors.addError(DdmUtils.printConsistencyChangeSetError(getChangeSet().getId()));
    }
    return validationErrors;
  }

  @Override
  public SqlStatement[] generateStatements(Database database) {
    if (DdmUtils.hasPubContext(this.getChangeSet())){
      this.getChangeSet().setIgnore(true);
      return new SqlStatement[0];
    }
    return super.generateStatements(database);
  }
}
