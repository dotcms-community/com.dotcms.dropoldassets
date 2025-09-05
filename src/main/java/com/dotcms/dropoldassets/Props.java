package com.dotcms.dropoldassets;

import io.vavr.control.Try;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * This utility class reads the configuration values specified in the {@code plugin.properties} file. Such parameters
 * are passed down to the Quartz job that take care of unlocking Contentlets throughout dotCMS.
 *
 * @author dotCMS
 * @since Aug 10, 2021
 */
public class Props {

  private static final String PROPERTY_FILE_NAME = "plugin.properties";
  private static Properties properties;

  static {
    properties = new Properties();
    try {
      InputStream in = Props.class.getResourceAsStream("/" + PROPERTY_FILE_NAME);
      if (in == null) {
        in = Props.class.getResourceAsStream("/com/dotcms/job/" + PROPERTY_FILE_NAME);
        if (in == null) {
          throw new FileNotFoundException(PROPERTY_FILE_NAME + " not found");
        }
      }
      properties.load(in);
    } catch (final FileNotFoundException e) {
      System.out.println("FileNotFoundException: " + PROPERTY_FILE_NAME + " not found");
      e.printStackTrace();
    } catch (final IOException e) {
      System.out.println("IOException: Can't read " + PROPERTY_FILE_NAME);
      e.printStackTrace();
    }
  }

  /**
   * Returns the value for the specified property key.
   *
   * @param key The property's key.
   * @return The property's value.
   */
  public static String getString(final String key) {
    return properties.getProperty(key);
  }

  /**
   * Returns the value for the specified property key. If such a property does not exist, the the default value will be
   * returned instead.
   *
   * @param key          The property's key.
   * @param defaultValue The property's default value.
   * @return The property's value or its default value.
   */
  public static String getString(final String key, final String defaultValue) {
    final String value = properties.getProperty(key);
    return (value == null) ? defaultValue : value;
  }

  public static boolean getBool(final String key, final boolean defaultValue) {
    return Try.of(() -> Boolean.parseBoolean(getString(key, Boolean.toString(defaultValue)))).getOrElse(defaultValue);
  }

  public static int getInt(final String key, final int defaultValue) {
    return Try.of(() -> Integer.parseInt(getString(key, ""))).getOrElse(defaultValue);
  }

  public static long getLong(final String key, final long defaultValue) {
    return Try.of(() -> Long.parseLong(getString(key, ""))).getOrElse(defaultValue);
  }

}
