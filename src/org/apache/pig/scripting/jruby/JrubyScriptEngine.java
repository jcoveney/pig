/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.scripting.jruby;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.rest.Main;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.utils.StructuresHelper.Pair;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.scripting.ScriptEngine;
import org.apache.pig.tools.pigstats.PigStats;
import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyBoolean;
import org.jruby.RubyInstanceConfig.CompileMode;
import org.jruby.embed.LocalContextScope;
import org.jruby.embed.LocalVariableBehavior;
import org.jruby.embed.ScriptingContainer;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

/**
 * Implementation of the script engine for Jruby, which facilitates the registration
 * of scripts as UDFs, and also provides information (via the nested class RubyFunctions)
 * on the registered functions.
 */
public class JrubyScriptEngine extends ScriptEngine {
    private static final Log LOG = LogFactory.getLog(JrubyScriptEngine.class);

    //TODO test if it is necessary to have a per script (or even per method) runtime. PRO: avoid collisions CON: a bunch of runtimes, which could be slow
    protected static final ScriptingContainer rubyEngine;

    private boolean isInitialized = false;

    static {
        rubyEngine = new ScriptingContainer(LocalContextScope.SINGLETHREAD, LocalVariableBehavior.PERSISTENT);
        rubyEngine.getProvider().getRubyInstanceConfig().setCompileMode(CompileMode.JIT); //consider using FORCE
    }

    /**
     * This is a static class which provides functionality around the functions that are registered with Pig.
     */
    static class RubyFunctions {
        /**
         * This cache maps function type to a map that maps path to a map of function name to the object
         * which contains information about that function. In the case of an EvalFunc, there is a special
         * function which encapsulates information about the function. In the case of an Accumulator or
         * Algebraic, it is just an instance of the Class object that extends AccumulatorPigUdf or
         * AlgebraicPigUdf, respectively.
         */
        private static Map<String, Map<String, Map<String, Object>>> functionsCache = Maps.newHashMap();

        private static Set<String> alreadyRunCache = Sets.newHashSet();

        private static Map<String, String> cacheFunction = Maps.newHashMap();

        static {
            //TODO use an enum instead?
            cacheFunction.put("evalfunc", "PigUdf.get_functions_to_register");
            cacheFunction.put("accumulator", "AccumulatorPigUdf.classes_to_register");
            cacheFunction.put("algebraic", "AlgebraicPigUdf.classes_to_register");

            functionsCache.put("evalfunc", new HashMap<String,Map<String,Object>>());
            functionsCache.put("accumulator", new HashMap<String,Map<String,Object>>());
            functionsCache.put("algebraic", new HashMap<String,Map<String,Object>>());
        }

        @SuppressWarnings("unchecked")
        private static Map<String,Object> getFromCache(String path, Map<String,Map<String,Object>> cacheToUpdate, String regCommand) {
            if (!alreadyRunCache.contains(path)) {
                for (Map.Entry<String, Map<String, Map<String, Object>>> entry : functionsCache.entrySet()) {
                    entry.getValue().remove(path);
                }

                rubyEngine.runScriptlet(getScriptAsStream(path), path);

                alreadyRunCache.add(path);
            }

            Map<String,Object> funcMap = cacheToUpdate.get(path);

            if (funcMap == null) {
                funcMap = (Map<String,Object>)rubyEngine.runScriptlet(regCommand);
                cacheToUpdate.put(path, funcMap);
            }

            return funcMap;
        }

        public static Map<String, Object> getFunctions(String cache, String path) {
            return getFromCache(path, functionsCache.get(cache), cacheFunction.get(cache));
        }
    }

    public static final String GEM_DIR_BASE_NAME = "gems_to_ship_123456"; //TODO move to PigConstants
    public static final String GEM_TAR_SYMLINK = "apreciousgemindeed.tar.gz"; //TODO move to PigConstants
    public static final String RUBY_LOAD_PATH_KEY = "pig.jruby.files.to.ship"; //TODO move to PigConstants
    public static final String RUBY_DEPENDENCY_NON_JAR_PATHS = "pig.jruby.simplified.paths.non.jar"; //TODO move to PigConstants


    // TODO need to bite the bullet and make this memory efficient. No being lazy :)
    public static File findCommonParent(List<String> files) {
        List<String> copy = Lists.newArrayList(files);
        Collections.sort(copy);
        File first = new File(copy.get(0)).getAbsoluteFile();
        File last = new File(copy.get(copy.size() - 1)).getAbsoluteFile();
        Deque<File> firstStack = new LinkedList<File>();
        Deque<File> lastStack = new LinkedList<File>();
        File parent = first;
        while ((parent = parent.getParentFile()) != null) {
            firstStack.add(parent);
        }

        parent = last;
        while ((parent = parent.getParentFile()) != null) {
            lastStack.add(parent);
        }

        parent = new File("/");
        while (!firstStack.isEmpty() && !lastStack.isEmpty()) {
            File firstParent = firstStack.pollLast();
            File lastParent = lastStack.pollLast();
            if (!firstParent.equals(lastParent)) { //TODO make sure equals ok to use here
                return parent;
            }
            parent = firstParent;
        }
        return parent;
    }

    private static final Pattern jarPathMatcher = Pattern.compile("\\.jar(!|$)");
    private static boolean pathIsAJar(String s) {
        return jarPathMatcher.matcher(s).find();
    }

    private static final Pattern fileInJarPathExtractor = Pattern.compile("(?:.*/)?(?:file:)?([^/]+)\\.jar!(.*)");
    private static Pair<String, String> getJarName(String s) {
        Matcher m = fileInJarPathExtractor.matcher(s);
        if (m.matches()) {
            return Pair.make(m.group(1), m.group(2));
        }
        return null;
    }

    private static final Pattern fileOnlyJarNameExtractor = Pattern.compile("(?:.*/)?(?:file:)?([^/]+)\\.jar$");
    private static String getJarNameOnly(String s) {
        Matcher m = fileOnlyJarNameExtractor.matcher(s);
        if (m.matches()) {
            return m.group(1);
        }
        return null;
    }

    public static void shipGems(PigContext pigContext, Configuration conf) throws IOException {
        //TODO do this all at the end
        //TODO for everything new in $", need to see what base it is off of in the load path $:
        //TODO anything new in the load path should be set accordingly in the mappers
        //TODO any new file in $" we need to find where in the load path it was found, and then set accordingly

        LOG.debug("Figuring out what dependencies will need to be shipped.");
        List<String> allLoadedFiles = rubyArrayToStringList((RubyArray)rubyEngine.runScriptlet("$\".clone"));
        List<String> fullLoadPath = rubyArrayToStringList((RubyArray)rubyEngine.runScriptlet("$LOAD_PATH.clone"));

        Set<String> allLoadedFilesPlusFilesFromGems = Sets.newHashSet();

        RubyArray additionalFilesFromGems = (RubyArray)rubyEngine.runScriptlet("Gem.loaded_specs.map {|k,v| Dir.glob(v.gem_dir+\"/lib/**/*\")}.flatten.reject {|x| File.directory? x}");
        for (Object o : additionalFilesFromGems) {
            allLoadedFilesPlusFilesFromGems.add(o.toString());
        }
        allLoadedFilesPlusFilesFromGems.addAll(allLoadedFiles);


        List<String> fullLoadPathNotJar = Lists.newArrayList(Collections2.filter(fullLoadPath, new Predicate<String>() {
            @Override
            public boolean apply(String s) {
                return !pathIsAJar(s);
            }
        }));

        LOG.debug("All files in $\": " + allLoadedFiles);
        LOG.debug("All directories in $LOAD_PATH: " + fullLoadPath);

        List<String> allLoadedFilesThatExist = Lists.newArrayList(Collections2.filter(allLoadedFilesPlusFilesFromGems, new Predicate<String>() {
            @Override
            public boolean apply(String s) {
                return new File(s).exists();
            }
        }));

        List<String> newLoadedFilesThatExist = Lists.newArrayList(Collections2.filter(allLoadedFilesThatExist, new Predicate<String>() {
            @Override
            public boolean apply(String s) {
                return !initialListOfLoadedFiles.contains(s);
            }
        }));

        List<String> newLoadedFilesThatExistNotInJars = Lists.newArrayList(Collections2.filter(newLoadedFilesThatExist, new Predicate<String>() {
            @Override
            public boolean apply(String s) {
                return !pathIsAJar(s);
            }
        }));

        LOG.debug("New files not in jars in $\": " + newLoadedFilesThatExistNotInJars);
        LOG.debug("Non-jars directories in $LOAD_PATH: " + fullLoadPathNotJar);

        File commonParent = findCommonParent(newLoadedFilesThatExistNotInJars);
        LOG.debug("Common parent for JRuby dependencies: " + commonParent);

        String loadPathValue = null;

        for (String s : fullLoadPathNotJar) {
            String loadPath = new File(s).getAbsolutePath();
            String parPath = commonParent.getAbsolutePath();
            if (loadPath.startsWith(parPath)) {
                String newPath = new File(GEM_DIR_BASE_NAME, loadPath.substring(parPath.length())).getPath();
                LOG.debug("Adding path to M/R JRuby load path: " + newPath);
                if (loadPathValue == null) {
                    loadPathValue  = newPath;
                } else {
                    loadPathValue  += "^^^" + newPath;
                }
            }
        }

        List<String> newLoadedFilesThatExistInJars = Lists.newArrayList(Collections2.filter(allLoadedFilesPlusFilesFromGems, new Predicate<String>() {
            @Override
            public boolean apply(String s) {
                return getJarName(s) != null;
            }
        }));

        List<String> fullLoadPathJar = Lists.newArrayList(Collections2.filter(fullLoadPath, new Predicate<String>() {
            @Override
            public boolean apply(String s) {
                return pathIsAJar(s);
            }
        }));

        LOG.debug("New files in jars in $\": " + newLoadedFilesThatExistInJars);
        LOG.debug("New jar directories in load path: " + fullLoadPathJar);
        Set<Pair<String,String>> jarFilesToLoad = Sets.newHashSet(); //TODO what if multiple jars have same name?
        Set<String> jarsToLoad = Sets.newHashSet();
        for (String s : newLoadedFilesThatExistInJars) {
            Pair<String, String> jarDetails = getJarName(s);
            //TODO could consider adding directly here IF we know for sure that
            //TODO any file inside a jar will have the jar base as a load path
            //TODO Q: is it possible to have a load path that ISN'T the base of a jar?
            jarsToLoad.add(jarDetails.getFirst());
            jarFilesToLoad.add(jarDetails);
        }

        for (String s : fullLoadPathJar) {
            String jar = getJarNameOnly(s);
            if (jarsToLoad.contains(jar)) {
                String newPath = new File(GEM_DIR_BASE_NAME, jar).getPath();
                LOG.debug("Adding synthetic jar path to load path: " + newPath);
                if (loadPathValue == null) {
                    loadPathValue  = newPath;
                } else {
                    loadPathValue  += "^^^" + newPath;
                }
            }
        }

        if (loadPathValue != null) {
            conf.set(RUBY_LOAD_PATH_KEY, loadPathValue);
            LOG.debug("Setting JobConf key ["+RUBY_LOAD_PATH_KEY+"] to: " + loadPathValue);
        } else {
            LOG.debug("No load path values to set in JobConf key: " + RUBY_LOAD_PATH_KEY);
        }

        File fout = File.createTempFile("tmp", ".tar.gz");
        fout.delete();
        fout.deleteOnExit();

        int commonParentSize = commonParent.getAbsolutePath().length() + 1;
        TarArchiveOutputStream os = new TarArchiveOutputStream(new GZIPOutputStream(new FileOutputStream(fout), 1024*1024));

        List<String> parentPathsToSerialize = Lists.newArrayList();

        LOG.debug("Beginning to archive JRuby dependencies (those NOT in jars) to ship.");
        for (String file : newLoadedFilesThatExistNotInJars) {
            LOG.debug("Attempting to archive file: " + file);
            String getTarName = file.substring(commonParentSize);
            getTarName = new File(GEM_DIR_BASE_NAME, getTarName).getPath();

            File entryFile = new File(file);
            TarArchiveEntry entry = new TarArchiveEntry(entryFile);

            String parentPathName = new File(getTarName).getParent();
            parentPathsToSerialize.add(parentPathName);

            String tarPath = new File(new File(Integer.toString(parentPathsToSerialize.size() - 1)), new File(file).getName()).getPath();

            entry.setName(tarPath);

            os.putArchiveEntry(entry);

            InputStream in = new FileInputStream(file);

            IOUtils.copy(in, os);

            in.close();
            os.closeArchiveEntry();
        }

        for (Pair<String,String> filePair : jarFilesToLoad) {
            String jar = filePair.getFirst();
            String pathInJar = filePair.getSecond();

            LOG.debug("Attempting to archive file ["+pathInJar+"] present in jar ["+jar+"]");

            String syntheticPath = new File(jar, pathInJar).getPath();
            String realTarPath = new File(GEM_DIR_BASE_NAME, syntheticPath).getPath();
            InputStream is = Main.class.getResourceAsStream(pathInJar);
            File tmp = File.createTempFile("tmp", "tmp");
            tmp.deleteOnExit();

            FileOutputStream fos = new FileOutputStream(tmp);
            IOUtils.copy(is, fos);
            fos.close();
            is.close();

            String parentPathName = new File(realTarPath).getParent();
            parentPathsToSerialize.add(parentPathName);
            String tarPath = new File(Integer.toString(parentPathsToSerialize.size() - 1), new File(pathInJar).getName()).getPath();

            TarArchiveEntry entry = new TarArchiveEntry(tmp);
            entry.setName(tarPath);

            os.putArchiveEntry(entry);

            InputStream in = new FileInputStream(tmp);

            IOUtils.copy(in, os);

            in.close();
            os.closeArchiveEntry();
        }

        os.finish();
        os.close();

        LOG.debug("Have successfully written to temporary .tar.gz file: " + fout);

        String[] parentPathsArray = parentPathsToSerialize.toArray(new String[parentPathsToSerialize.size()]);
        conf.set(RUBY_DEPENDENCY_NON_JAR_PATHS, ObjectSerializer.serialize(parentPathsArray));

        shipTarToDistributedCache(fout, pigContext, conf);
    }

    private static void shipTarToDistributedCache(File tar, PigContext pigContext, Configuration conf) {
        DistributedCache.createSymlink(conf); // we will read using symlinks
        Path src = new Path(tar.toURI());
        Path dst;
        try {
            dst = FileLocalizer.getTemporaryPath(pigContext);
        } catch (IOException e) {
            throw new RuntimeException("Error getting temporary path in HDFS", e);
        }
        FileSystem fs;
        try {
            fs = dst.getFileSystem(conf);
        } catch (IOException e) {
            throw new RuntimeException("Unable to get FileSystem", e);
        }
        try {
            fs.copyFromLocalFile(src, dst);
        } catch (IOException e) {
            throw new RuntimeException("Unable to copy from local filesystem to HDFS, src = "
                    + src + ", dst = " + dst, e);
        }

        String destination = dst.toString() + "#" + GEM_TAR_SYMLINK;

        try {
            DistributedCache.addCacheFile(new URI(destination), conf);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Unable to add file to distributed cache: " + destination, e);
        }
    }

    private static boolean haveCopiedFromDistributedCache = false;

    public static void copyFromDistributedCache() throws IOException {
        if (!haveCopiedFromDistributedCache) {
            haveCopiedFromDistributedCache = true;
        }

        File gemTar = new File(GEM_TAR_SYMLINK);
        if (!gemTar.exists()) {
            return; // there are no gems
        }

        Configuration conf = UDFContext.getUDFContext().getJobConf();

        File gemDir = Files.createTempDir();
        gemDir.deleteOnExit();
        LOG.debug("Temporary gem location: " + gemDir);
        TarArchiveInputStream is = new TarArchiveInputStream(new GZIPInputStream(new FileInputStream(gemTar)));

        String[] parentPathsArray = (String[]) ObjectSerializer.deserialize(conf.get(RUBY_DEPENDENCY_NON_JAR_PATHS));

        TarArchiveEntry entry;
        while ((entry = is.getNextTarEntry()) != null) {
            String entryName = entry.getName();
            LOG.debug("Processing next entry: " + entryName);
            int fileIndex = Integer.parseInt(new File(entryName).getParent());
            File realPath = new File(parentPathsArray[fileIndex]);
            String realFile = new File(realPath, new File(entryName).getName()).getPath();
            LOG.debug("Real path for entry: " + realFile);

            File fileToWriteTo = new File(gemDir, realFile);
            File parent = fileToWriteTo.getParentFile();
            if (!parent.exists()) {
                parent.mkdirs();
            }
            LOG.debug("Attempting to write to temporary location: " + fileToWriteTo);

            OutputStream os = new BufferedOutputStream(new FileOutputStream(fileToWriteTo));
            byte[] buf = new byte[1024 * 1024];

            while (true) {
                int numRead = is.read(buf, 0, buf.length);

                if (numRead == -1) {
                    break;
                }

                os.write(buf, 0, numRead);
            }

            os.close();
        }
        is.close();

        String loadPaths = conf.get(RUBY_LOAD_PATH_KEY);
        if (loadPaths != null) {
            for (String loadPath : loadPaths.split("\\^\\^\\^")) {
                String path = new File(gemDir, loadPath).getAbsolutePath();
                LOG.debug("Adding path to load path: " + path);
                rubyEngine.runScriptlet("$LOAD_PATH.unshift(\""+path+"\")");
            }
        }
    }

    private Map<String, Set<String>> scriptsInNamespace = Maps.newHashMap();
    private Set<String> scriptsAlreadyRun = Sets.newHashSet();
    private static List<String> initialListOfLoadedFiles;

    @SuppressWarnings("unchecked")
    private static List<String> rubyArrayToStringList(RubyArray array) {
        return Lists.transform(array, new Function<Object,String>() {
            @Override
            public String apply(Object o) {
                return new File(o.toString()).getAbsolutePath();
            }
        });
    }

    /**
     * Evaluates the script containing ruby udfs to determine what udfs are defined as well as
     * what libaries and other external resources are necessary. These libraries and resources
     * are then packaged with the job jar itself.
     */
    @Override
    public void registerFunctions(String path, String namespace, PigContext pigContext) throws IOException {
        if (!isInitialized) {
            initialListOfLoadedFiles = rubyArrayToStringList((RubyArray)rubyEngine.runScriptlet("$\".clone"));
            LOG.debug("Initial $\": " + initialListOfLoadedFiles);

            pigContext.scriptJars.add(getJarPath(Ruby.class));
            rubyEngine.runScriptlet("require 'pigudf'");
            rubyEngine.runScriptlet(getScriptAsStream("pigudf.rb"), "pigudf.rb");
            scriptsAlreadyRun.add("pigudf.rb");
            //pigContext.addScriptFile("pigudf.rb", "pigudf.rb");
            isInitialized = true;
        }

        Set<String> scriptsInThisNamespace = scriptsInNamespace.get(namespace);
        if (scriptsInThisNamespace != null && scriptsInThisNamespace.contains(path)) {
            LOG.warn("Script at path ["+path+"] already in namespace ["+namespace+"]. Skipping.");
            return;
        }
        if (scriptsInThisNamespace == null) {
            scriptsInThisNamespace = Sets.newHashSet();
            scriptsInNamespace.put(namespace, scriptsInThisNamespace);
        }
        scriptsInThisNamespace.add(path);

        if (!scriptsAlreadyRun.contains(path)) {
            LOG.debug("Running JRuby script: " + path);
            rubyEngine.runScriptlet(getScriptAsStream(path), path);
            scriptsAlreadyRun.add(path);
        }

        for (Map.Entry<String,Object> entry : RubyFunctions.getFunctions("evalfunc", path).entrySet()) {
            String method = entry.getKey();

            FuncSpec funcspec = new FuncSpec(JrubyEvalFunc.class.getCanonicalName() + "('" + path + "','" + method +"')");
            pigContext.registerFunction(namespace + "." + method, funcspec);
        }

        for (Map.Entry<String,Object> entry : RubyFunctions.getFunctions("accumulator", path).entrySet()) {
            String method = entry.getKey();

            if (rubyEngine.callMethod(entry.getValue(), "check_if_necessary_methods_present", RubyBoolean.class).isFalse())
                throw new RuntimeException("Method " + method + " does not have all of the required methods present!");

            pigContext.registerFunction(namespace + "." + method, new FuncSpec(JrubyAccumulatorEvalFunc.class.getCanonicalName() + "('" + path + "','" + method +"')"));
        }

        for (Map.Entry<String,Object> entry : RubyFunctions.getFunctions("algebraic", path).entrySet()) {
            String method = entry.getKey();

            if (rubyEngine.callMethod(entry.getValue(), "check_if_necessary_methods_present", RubyBoolean.class).isFalse())
                throw new RuntimeException("Method " + method + " does not have all of the required methods present!");

            Schema schema = PigJrubyLibrary.rubyToPig(rubyEngine.callMethod(entry.getValue(), "get_output_schema", RubySchema.class));
            String canonicalName = JrubyAlgebraicEvalFunc.class.getCanonicalName() + "$";

            // In the case of an Algebraic UDF, a type specific EvalFunc is necessary (ie not EvalFunc<Object>), so we
            // inspect the type and instantiated the proper class.
            switch (schema.getField(0).type) {
                case DataType.BAG: canonicalName += "Bag"; break;
                case DataType.TUPLE: canonicalName += "Tuple"; break;
                case DataType.CHARARRAY: canonicalName += "Chararray"; break;
                case DataType.DOUBLE: canonicalName += "Double"; break;
                case DataType.FLOAT: canonicalName += "Float"; break;
                case DataType.INTEGER: canonicalName += "Integer"; break;
                case DataType.LONG: canonicalName += "Long"; break;
                case DataType.DATETIME: canonicalName += "DateTime"; break;
                case DataType.MAP: canonicalName += "Map"; break;
                case DataType.BYTEARRAY: canonicalName += "DataByteArray"; break;
                default: throw new ExecException("Unable to instantiate Algebraic EvalFunc " + method + " as schema type is invalid");
            }

            canonicalName += "JrubyAlgebraicEvalFunc";

            pigContext.registerFunction(namespace + "." + method, new FuncSpec(canonicalName + "('" + path + "','" + method +"')"));
        }
    }

    /**
     * Consults the scripting container, after the script has been evaluated, to
     * determine what dependencies to ship.
     * <p>
     * FIXME: Corner cases like the following: "def foobar; require 'json'; end"
     * are NOT dealt with using this method
     */
    private HashSet<String> libsToShip() {
        RubyArray loadedLibs = (RubyArray)rubyEngine.get("$\"");
        RubyArray loadPaths = (RubyArray)rubyEngine.get("$LOAD_PATH");
        // Current directory first
        loadPaths.add(0, "");

        HashSet<String> toShip = new HashSet<String>();
        HashSet<Object> shippedLib = new HashSet<Object>();

        for (Object loadPath : loadPaths) {
            for (Object lib : loadedLibs) {
                if (lib.toString().equals("pigudf.rb"))
                    continue;
                if (shippedLib.contains(lib))
                    continue;
                String possiblePath = (loadPath.toString().isEmpty()?"":loadPath.toString() +
                        File.separator) + lib.toString();
                if ((new File(possiblePath)).exists()) {
                    // remove prefix ./
                    toShip.add(possiblePath.startsWith("./")?possiblePath.substring(2):
                        possiblePath);
                    shippedLib.add(lib);
                }
            }
        }
        return toShip;
    }

    private static List<File> listRecursively(File directory) {
        File[] entries = directory.listFiles();

        ArrayList<File> files = new ArrayList<File>();

        // Go over entries
        for (File entry : entries) {
            files.add(entry);
            if (entry.isDirectory()) {
                files.addAll(listRecursively(entry));
            }
        }
        return files;
    }

    @Override
    protected Map<String, List<PigStats>> main(PigContext pigContext, String scriptFile) throws IOException {
        throw new UnsupportedOperationException("Unimplemented");
    }

    @Override
    protected String getScriptingLang() {
        return "jruby";
    }

    @Override
    protected Map<String, Object> getParamsFromVariables() throws IOException {
        Map<String, Object> vars = Maps.newHashMap();
        return vars;
    }
}
