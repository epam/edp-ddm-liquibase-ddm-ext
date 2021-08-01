package liquibase.change.core;

import java.util.ArrayList;
import java.util.List;
import liquibase.DdmConstants;
import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.DdmTableConfig;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.DdmPartialUpdateStatement;

/**
 * Creates a new partialUpdate.
 */

@DatabaseChange(name="partialUpdate", description = "partialUpdate - partial update", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class DdmPartialUpdateChange extends AbstractChange {

    private List<DdmTableConfig> tables = new ArrayList<>();
    private String name;

    private ValidationErrors validateDoubledTables() {
        ValidationErrors validationErrors = new ValidationErrors();
        List<String> tables = new ArrayList<>();

        for (DdmTableConfig table : getTables()) {
            if (tables.contains(table.getName())) {
                validationErrors.addError("There is doubled table=" + table.getName());
            } else {
                tables.add(table.getName());
            }
        }

        return validationErrors;
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.addAll(validateDoubledTables());

        return validationErrors;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {

        DdmPartialUpdateStatement statement = new DdmPartialUpdateStatement(getName());
        statement.setTables(getTables());

        return new SqlStatement[]{
            statement
        };
    }

    @Override
    public String getConfirmationMessage() {
        return "Partial update has been set";
    }

    @Override
    public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
        super.load(parsedNode, resourceAccessor);

        for (ParsedNode child : parsedNode.getChildren()) {
            if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_TABLE)) {
                DdmTableConfig table = new DdmTableConfig();
                table.load(child, resourceAccessor);
                addTable(table);
            }
        }
    }

    public void addTable(DdmTableConfig table) {
        this.tables.add(table);
    }

    public List<DdmTableConfig> getTables() {
        return tables;
    }

    public void setTables(List<DdmTableConfig> tables) {
        this.tables = tables;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}