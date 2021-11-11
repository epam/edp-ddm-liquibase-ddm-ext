# liquibase-ddm-ext

Custom extension, which alter the behaviour of the original Liquibase in order to be able to deal with custom tags. Liquibase schema, which contains all custom tags, attributes and xml types, can be found in the `liquibase-ext-schema` repository.

### Usage
List of allowed commands with their arguments can be found [here](https://docs.liquibase.com/commands/home.html)  
Example:
```bash
liquibase --contexts=pub,sub --driver=org.postgresql.Driver --changeLogFile=main-liquibase.xml --url=jdbc:postgresql://localhost:5432/<dbname> --username=<username> --password=<password> --labels=!citus update
```

### Local deployment:
###### Prerequisites:

* Postgres database is configured and running
* Liquibase is installed
* Postgres JDBC driver is downloaded

###### Steps:

1. Run extension  
   Two options:
    * JAR file approach:
        * package application into jar file with `mvn clean package`
        * move created jar to `$LIQUIBASE_HOME/lib` folder
        * add JDBC driver to the same folder
        * run `liquibase ...` command with custom arguments
    * Java console app approach:
        * add custom arguments to `liquibase.properties` file or to java run args
        * run java app with the entry point in `liquibase.integration.commandline.Main.main(..)`
