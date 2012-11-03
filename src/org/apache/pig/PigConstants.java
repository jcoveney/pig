package org.apache.pig;

public class PigConstants {
    private PigConstants() {}

    /**
     * This key is used in the job conf to let the various jobs know what code was
     * generated.
     */
    public static final String GENERATED_CLASSES_KEY = "pig.schematuple.classes";

    /**
     * This key is used when a job is run in local mode to pass the location of the generated code
     * from the front-end to the "back-end" (which, in this case, is in the same JVM).
     */
    public static final String LOCAL_CODE_DIR = "pig.schematuple.local.dir";

    // This makes it easy to turn SchemaTuple on globally.
    public static final boolean SCHEMA_TUPLE_ON_BY_DEFAULT = false;

    // These values are used in JrubyScriptEngine
    public static final String GEM_DIR_BASE_NAME = "gems_to_ship_123456";
    public static final String GEM_TAR_SYMLINK = "apreciousgemindeed.tar.gz";
    public static final String RUBY_LOAD_PATH_KEY = "pig.jruby.files.to.ship";
    public static final String RUBY_DEPENDENCY_NON_JAR_PATHS = "pig.jruby.simplified.paths";
}