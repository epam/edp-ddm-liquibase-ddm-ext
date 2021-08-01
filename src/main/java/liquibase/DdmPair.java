package liquibase;

import java.util.Objects;

public class DdmPair {
    private String key;
    private String value;

    public DdmPair(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return "DdmPair {" +
            "key='" + key + "'" +
            ", value='" + value + "'}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DdmPair ddmPair = (DdmPair) o;
        return Objects.equals(key, ddmPair.key) && Objects
            .equals(value, ddmPair.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
