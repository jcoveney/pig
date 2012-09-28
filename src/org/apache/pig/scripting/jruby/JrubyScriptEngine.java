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
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.scripting.ScriptEngine;
import org.apache.pig.tar.TarUtils;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;
import org.apache.tools.tar.TarOutputStream;
import org.jruby.CompatVersion;
import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyBoolean;
import org.jruby.embed.LocalContextScope;
import org.jruby.embed.LocalVariableBehavior;
import org.jruby.embed.ScriptingContainer;

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
        rubyEngine.setCompatVersion(CompatVersion.RUBY1_9);
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

        private static void setHasAlreadyRun(String path) {
            alreadyRunCache.add(path);
        }

        @SuppressWarnings("unchecked")
        private static Map<String,Object> getFromCache(String path, Map<String,Map<String,Object>> cacheToUpdate, String regCommand) {
            if (!alreadyRunCache.contains(path)) {
                for (Map.Entry<String, Map<String, Map<String, Object>>> entry : functionsCache.entrySet())
                    entry.getValue().remove(path);

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

    private static Map<String, File> gemsToShip = Maps.newHashMap();
    public static final String GEM_DIR_BASE_NAME = "gems_to_ship_123456"; //TODO move to PigConstants
    public static final String GEM_TAR_SYMLINK = "apreciousgemindeed.tar.gz"; //TODO move to PigConstants

    //TODO add logging!

    public static void shipGems(PigContext pigContext, Configuration conf) throws IOException {
        if (gemsToShip.isEmpty()) {
            return;
        }
        shipTarToDistributedCache(tarGemsToShip(Sets.newHashSet(gemsToShip.values())), pigContext, conf);
    }

    // Another general option would be to add them individually to the distributed cache as they come
    private static File tarGemsToShip(Set<File> gems) throws IOException {
        File fout = File.createTempFile("tmp", ".tar.gz");
        fout.delete();
        // do I need to buffer as well or does GZIP already?
        TarOutputStream os = new TarOutputStream(new GZIPOutputStream(new FileOutputStream(fout)));

        for (File f : gems) {
            TarUtils.tarFile(GEM_DIR_BASE_NAME, f.getParentFile(), new File(f, "lib"), os);
        }
        os.close();
        return fout;
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
        File gemDir = Files.createTempDir();
        gemDir.deleteOnExit();
        TarInputStream is = new TarInputStream(new GZIPInputStream(new FileInputStream(gemTar)));
        TarEntry entry;
        while ((entry = is.getNextEntry()) != null) {
            File fileToWriteTo = new File(gemDir, entry.getFile().getPath());
            OutputStream os = new BufferedOutputStream(new FileOutputStream(fileToWriteTo));
            is.copyEntryContents(os);
            os.close();
        }
        is.close();
        List<String> loadPaths = rubyEngine.getLoadPaths();
        for (File f : gemDir.listFiles()) {
            loadPaths.add(new File(f, "lib").getAbsolutePath());
        }
        rubyEngine.setLoadPaths(loadPaths);
    }

    public static void addShippedGemsToLoadPath() {

    }

    /**
     * Evaluates the script containing ruby udfs to determine what udfs are defined as well as
     * what libaries and other external resources are necessary. These libraries and resources
     * are then packaged with the job jar itself.
     */
    @Override
    public void registerFunctions(String path, String namespace, PigContext pigContext) throws IOException {
        if (!isInitialized) {
            pigContext.scriptJars.add(getJarPath(Ruby.class));
            pigContext.addScriptFile("pigudf.rb", "pigudf.rb");
            isInitialized = true;
        }

        rubyEngine.runScriptlet(getScriptAsStream(path), path);
        RubyFunctions.setHasAlreadyRun(path);
        RubyArray list = (RubyArray)rubyEngine.runScriptlet("Gem.loaded_specs.map {|k,v| [k,v.gem_dir]}");
        for (Object o : list) {
            RubyArray innerList = (RubyArray)o;
            String gemName = innerList.get(0).toString();
            if (gemsToShip.get(gemName) == null) {
                gemsToShip.put(gemName, new File(innerList.get(1).toString()));
            }
        }

        for (Map.Entry<String,Object> entry : RubyFunctions.getFunctions("evalfunc", path).entrySet()) {
            String method = entry.getKey();

            String functionType = rubyEngine.callMethod(entry.getValue(), "name", String.class); //TODO why is this here? use!

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

        // Determine what external dependencies to ship with the job jar
        HashSet<String> toShip = libsToShip();
        for (String lib : toShip) {
            File libFile = new File(lib);
            if (lib.endsWith(".rb")) {
                pigContext.addScriptFile(lib);
            } else if (libFile.isDirectory()) {
                //
                // Need to package entire contents of directory
                //
                List<File> files = listRecursively(libFile);
                for (File file : files) {
                    if (file.isDirectory()) {
                        continue;
                    } else if (file.getName().endsWith(".jar") || file.getName().endsWith(".zip")) {
                        pigContext.scriptJars.add(file.getPath());
                    } else {
                        String localPath = libFile.getName() + file.getPath().replaceFirst(libFile.getPath(), "");
                        pigContext.addScriptFile(localPath, file.getPath());
                    }
                }
            } else {
                pigContext.scriptJars.add(lib);
            }
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
