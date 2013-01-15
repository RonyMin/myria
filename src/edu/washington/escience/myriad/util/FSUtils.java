package edu.washington.escience.myriad.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Filesystem util methods.
 * */
public final class FSUtils {

  /**
   * util classes are not instantiable.
   * */
  private FSUtils() {
  }

  /**
   * Delete the pathToDirectory. Return only if the directory is actually get deleted on the disk.
   * */
  public static void blockingDeleteDirectory(String pathToDirectory) {
    final File testBaseFolderF = new File(pathToDirectory);
    try {
      FSUtils.deleteFileFolder(testBaseFolderF);
    } catch (final IOException e) {
      e.printStackTrace();
    }
    boolean finishClean = false;
    while (!finishClean) {
      finishClean = !testBaseFolderF.exists();
      if (!finishClean) {
        try {
          Thread.sleep(100);
        } catch (final InterruptedException e) {
          e.printStackTrace();
          Thread.currentThread().interrupt();
        }
      }
    }

  }

  public static void deleteFileFolder(final File f) throws IOException {
    if (!f.exists()) {
      return;
    }
    if (f.isDirectory()) {
      for (final File c : f.listFiles()) {
        deleteFileFolder(c);
      }
    }
    if (!f.delete()) {
      throw new FileNotFoundException("Failed to delete file: " + f);
    }
  }

  /**
   * @param dest will be replaced if exists and override
   */
  public static void copyFileFolder(final File source, final File dest, final boolean override) throws IOException {
    if (dest.exists()) {
      if (!override) {
        return;
      } else {
        deleteFileFolder(dest);
      }
    }

    if (source.isDirectory()) {
      dest.mkdirs();
      final File[] children = source.listFiles();
      for (final File child : children) {
        copyFileFolder(child, new File(dest.getAbsolutePath() + "/" + child.getName()), override);
      }
    } else {
      InputStream in = null;
      OutputStream out = null;
      try {
        in = new FileInputStream(source);
        out = new FileOutputStream(dest);

        // Transfer bytes from in to out
        final byte[] buf = new byte[1024];
        int len;
        while ((len = in.read(buf)) > 0) {
          out.write(buf, 0, len);
        }
      } finally {
        if (in != null) {
          in.close();
        }
        if (out != null) {
          out.close();
        }
      }
    }
  }

  public static void writeFile(final File f, final String content) throws IOException {
    final FileOutputStream o = new FileOutputStream(f);
    o.write(content.getBytes());
    o.close();
  }
}