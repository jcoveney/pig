package org.apache.pig.tar;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TarUtils {
    private static final Log LOG = LogFactory.getLog(TarUtils.class);

    public static void tarFile(String prefix, File baseDir, File file, TarArchiveOutputStream os) throws IOException {
        File absBase = baseDir.getAbsoluteFile();
        File absFile = file.getAbsoluteFile();

        File parent = absFile;
        while (!parent.equals(baseDir)) {
            if ((parent = parent.getParentFile()) == null) {
                throw new RuntimeException("Given base directory ["+baseDir+"] is not a parent of File to tar ["+file+"]");
            }
        }

        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                tarFile(prefix, baseDir, f, os);
            }
        } else {
            TarArchiveEntry entry = new TarArchiveEntry(file);

            String name = absFile.getAbsolutePath().substring(absBase.getAbsolutePath().length() + 1);
            entry.setName((prefix != null ? prefix + "/" : "") + name);

            os.putArchiveEntry(entry);

            InputStream in = new FileInputStream(file);

            byte[] buf = new byte[2048];
            int written;
            while ((written = in.read(buf)) != -1) {
                os.write(buf, 0, written);
            }

            in.close();
            os.closeArchiveEntry();
            os.flush();
        }
    }
}