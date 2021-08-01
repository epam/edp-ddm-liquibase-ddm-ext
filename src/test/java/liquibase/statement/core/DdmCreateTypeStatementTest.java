package liquibase.statement.core;

import liquibase.change.DdmColumnConfig;
import liquibase.change.DdmLabelConfig;
import liquibase.change.DdmTypeConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmCreateTypeStatementTest {
    @Test
    @DisplayName("Check statements - adding columns")
    public void columns() {
        DdmTypeConfig asComposite = new DdmTypeConfig();

        DdmColumnConfig column1 = new DdmColumnConfig();
        column1.setName("column1");
        column1.setType("type1");
        asComposite.addColumn(column1);

        DdmColumnConfig column2 = new DdmColumnConfig();
        column2.setName("column2");
        column2.setType("type2");
        asComposite.addColumn(column2);

        DdmCreateTypeStatement statement = new DdmCreateTypeStatement("name", asComposite, null);

        Assertions.assertEquals(2, statement.getAsComposite().getColumns().size());
    }

    @Test
    @DisplayName("Check statements - adding labels")
    public void labels() {
        DdmTypeConfig asEnum = new DdmTypeConfig();

        DdmLabelConfig label = new DdmLabelConfig();
        label.setLabel("label1");
        label.setTranslation("translation1");
        asEnum.addLabel(label);

        label = new DdmLabelConfig();
        label.setLabel("label2");
        label.setTranslation("translation2");
        asEnum.addLabel(label);

        DdmCreateTypeStatement statement = new DdmCreateTypeStatement("name", null, asEnum);

        Assertions.assertEquals(2, statement.getAsEnum().getLabels().size());
    }
}