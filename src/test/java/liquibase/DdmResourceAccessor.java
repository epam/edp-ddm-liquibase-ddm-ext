package liquibase;

import liquibase.resource.ClassLoaderResourceAccessor;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;

public class DdmResourceAccessor extends ClassLoaderResourceAccessor {

    public DdmResourceAccessor() throws Exception {
        super(new URLClassLoader(new URL[]{
            new File(".", "src/test/resources").getCanonicalFile().toURI().toURL(),
        }, null));
    }
}
