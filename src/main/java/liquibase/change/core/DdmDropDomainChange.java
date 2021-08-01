package liquibase.change.core;

import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.DatabaseChangeProperty;
import liquibase.database.Database;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.DdmDropDomainStatement;

/**
 * Drops an existing domain.
 */
@DatabaseChange(name="dropDomain", description = "Drops an existing domain", priority = ChangeMetaData.PRIORITY_DEFAULT, appliesTo = "domain")
public class DdmDropDomainChange extends AbstractChange {

    private String name;

    @DatabaseChangeProperty(mustEqualExisting = "domain", description = "Name of the domain to drop")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        return new SqlStatement[]{
            new DdmDropDomainStatement(getName())
        };
    }

    @Override
    public String getConfirmationMessage() {
        return "Domain " + getName() + " dropped";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return STANDARD_CHANGELOG_NAMESPACE;
    }
}
