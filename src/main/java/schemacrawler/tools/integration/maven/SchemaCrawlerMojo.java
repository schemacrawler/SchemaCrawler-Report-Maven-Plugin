/*
 * #%L
 * SchemaCrawler Maven Plugin
 * %%
 * Copyright (C) 2011 SchemaCrawler
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as 
 * published by the Free Software Foundation, either version 3 of the 
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Lesser Public License for more details.
 * 
 * You should have received a copy of the GNU General Lesser Public 
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/lgpl-3.0.html>.
 * #L%
 */
package schemacrawler.tools.integration.maven;


import static sf.util.Utility.isBlank;
import static sf.util.Utility.readFully;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.doxia.sink.Sink;
import org.apache.maven.doxia.siterenderer.Renderer;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.apache.maven.reporting.AbstractMavenReport;
import org.apache.maven.reporting.MavenReportException;

import schemacrawler.schemacrawler.Config;
import schemacrawler.schemacrawler.ConnectionOptions;
import schemacrawler.schemacrawler.DatabaseConnectionOptions;
import schemacrawler.schemacrawler.RegularExpressionRule;
import schemacrawler.schemacrawler.SchemaCrawlerException;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.tools.executable.Executable;
import schemacrawler.tools.executable.SchemaCrawlerExecutable;
import schemacrawler.tools.options.InfoLevel;
import schemacrawler.tools.options.OutputOptions;
import schemacrawler.tools.options.TextOutputFormat;
import schemacrawler.tools.text.schema.SchemaTextOptions;
import schemacrawler.tools.text.schema.SchemaTextOptionsBuilder;
import sf.util.ObjectToString;

/**
 * Generates a SchemaCrawler report of the database.
 */
@Mojo(name = "schemacrawler", requiresReports = true, threadSafe = true)
public class SchemaCrawlerMojo
  extends AbstractMavenReport
{

  private static final String INCLUDE_ALL = ".*";
  private static final String INCLUDE_NONE = "";

  @Component
  private MavenProject project;

  /**
   * Config file.
   */
  @Parameter(property = "config",
      defaultValue = "schemacrawler.config.properties", required = true)
  private String config;

  /**
   * Additional config file.
   */
  @Parameter(property = "additional-config",
      defaultValue = "schemacrawler.additional.config.properties")
  private String additionalConfig;

  /**
   * JDBC driver class name.
   */
  @Parameter(property = "driver", defaultValue = "${schemacrawler.driver}",
      required = true)
  private String driver;

  /**
   * Database connection string.
   */
  @Parameter(property = "url", defaultValue = "${schemacrawler.url}",
      required = true)
  private String url;

  /**
   * Database connection user name.
   */
  @Parameter(property = "user", defaultValue = "${schemacrawler.user}",
      required = true)
  private String user;

  /**
   * Database connection user password.
   */
  @Parameter(property = "password", defaultValue = "${schemacrawler.password}")
  private String password;

  /**
   * SchemaCrawler command.
   */
  @Parameter(property = "command", required = true)
  private String command;

  /**
   * The plugin dependencies.
   */
  @Parameter(property = "plugin.artifacts", required = true, readonly = true)
  private List<Artifact> pluginArtifacts;

  /**
   * Sort tables alphabetically.
   */
  @Parameter(property = "sorttables", defaultValue = "true")
  private boolean sorttables;

  /**
   * Sort columns in a table alphabetically.
   */
  @Parameter(property = "sortcolumns", defaultValue = "false")
  private boolean sortcolumns;

  /**
   * Sort parameters in a routine alphabetically.
   */
  @Parameter(property = "sortinout", defaultValue = "false")
  private boolean sortinout;

  /**
   * The info level determines the amount of database metadata retrieved, and
   * also determines the time taken to crawl the schema.
   */
  @Parameter(property = "infolevel", defaultValue = "standard", required = true)
  private String infolevel;

  /**
   * Schemas to include.
   */
  @Parameter(property = "schemas", defaultValue = INCLUDE_ALL)
  private String schemas;

  /**
   * Comma-separated list of table types of
   * TABLE,VIEW,SYSTEM_TABLE,GLOBAL_TEMPORARY,LOCAL_TEMPORARY,ALIAS
   */
  @Parameter(property = "table_types")
  private String table_types;

  /**
   * Regular expression to match fully qualified table names, in the form
   * "CATALOGNAME.SCHEMANAME.TABLENAME" - for example, .*\.C.*|.*\.P.* Tables
   * that do not match the pattern are not displayed.
   */
  @Parameter(property = "tables", defaultValue = INCLUDE_ALL)
  private String tables;

  /**
   * Regular expression to match fully qualified column names, in the form
   * "CATALOGNAME.SCHEMANAME.TABLENAME.COLUMNNAME" - for example,
   * .*\.STREET|.*\.PRICE matches columns named STREET or PRICE in any table
   * Columns that match the pattern are not displayed
   */
  @Parameter(property = "excludecolumns", defaultValue = INCLUDE_NONE)
  private String excludecolumns;

  /**
   * Regular expression to match fully qualified routine names, in the form
   * "CATALOGNAME.SCHEMANAME.ROUTINENAME" - for example, .*\.C.*|.*\.P.* matches
   * any routines whose names start with C or P Routines that do not match the
   * pattern are not displayed
   */
  @Parameter(property = "routines", defaultValue = INCLUDE_ALL)
  private String routines;

  /**
   * Regular expression to match fully qualified parameter names. Parameters
   * that match the pattern are not displayed
   */
  @Parameter(property = "excludeinout", defaultValue = INCLUDE_NONE)
  private String excludeinout;

  /**
   * {@inheritDoc}
   *
   * @see org.apache.maven.reporting.MavenReport#getDescription(java.util.Locale)
   */
  @Override
  public String getDescription(final Locale locale)
  {
    return "SchemaCrawler Report";
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.maven.reporting.MavenReport#getName(java.util.Locale)
   */
  @Override
  public String getName(final Locale locale)
  {
    return "SchemaCrawler Report";
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.maven.reporting.MavenReport#getOutputName()
   */
  @Override
  public String getOutputName()
  {
    return "schemacrawler";
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.maven.reporting.AbstractMavenReport#executeReport(java.util.Locale)
   */
  @Override
  protected void executeReport(final Locale locale)
    throws MavenReportException
  {
    final Log logger = getLog();

    try
    {
      fixClassPath();

      final File outputFile = executeSchemaCrawler();

      final Sink sink = getSink();
      logger.info(sink.getClass().getName());

      sink
        .rawText("<link rel=\"stylesheet\" href=\"./css/schemacrawler-output.css\" type=\"text/css\"/>\n");
      sink.rawText(readFully(new FileReader(outputFile)));

      sink.flush();
    }
    catch (final Exception e)
    {
      throw new MavenReportException("Error executing SchemaCrawler command "
                                     + command, e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.maven.reporting.AbstractMavenReport#getOutputDirectory()
   */
  @Override
  protected String getOutputDirectory()
  {
    return null; // Unused in the Maven API
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.maven.reporting.AbstractMavenReport#getProject()
   */
  @Override
  protected MavenProject getProject()
  {
    return project;
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.maven.reporting.AbstractMavenReport#getSiteRenderer()
   */
  @Override
  protected Renderer getSiteRenderer()
  {
    return null; // Unused in the Maven API
  }

  /**
   * Load configuration files, and add in other configuration options.
   *
   * @return SchemaCrawler command configuration
   */
  private Config createAdditionalConfiguration()
  {
    final SchemaTextOptions textOptions = new SchemaTextOptions();
    textOptions.setNoHeader(true);
    textOptions.setNoFooter(true);
    textOptions.setAlphabeticalSortForTables(sorttables);
    textOptions.setAlphabeticalSortForTableColumns(sortcolumns);
    textOptions.setAlphabeticalSortForRoutineColumns(sortinout);
    final Config textOptionsConfig = new SchemaTextOptionsBuilder(textOptions)
      .toConfig();

    try
    {
      final Config additionalConfiguration = Config.load(config,
                                                         additionalConfig);
      additionalConfiguration.putAll(textOptionsConfig);
      return additionalConfiguration;
    }
    catch (final IOException e)
    {
      final Log logger = getLog();
      logger.warn(e);

      return textOptionsConfig;
    }
  }

  private ConnectionOptions createConnectionOptions()
    throws SchemaCrawlerException, MavenReportException
  {
    try
    {
      Class.forName(driver);
    }
    catch (final ClassNotFoundException e)
    {
      throw new MavenReportException("Cannot load JDBC driver", e);
    }

    final ConnectionOptions connectionOptions = new DatabaseConnectionOptions(url);
    connectionOptions.setUser(user);
    connectionOptions.setPassword(password);
    return connectionOptions;
  }

  private OutputOptions createOutputOptions(final Path outputFile)
  {
    final OutputOptions outputOptions = new OutputOptions();
    outputOptions.setOutputFormatValue(TextOutputFormat.html.name());
    outputOptions.setOutputFile(outputFile);
    return outputOptions;
  }

  private Collection<String> splitTableTypes(final String tableTypesString)
  {
    final Collection<String> tableTypes;
    if (tableTypesString != null)
    {
      tableTypes = new HashSet<>();
      final String[] tableTypeStrings = tableTypesString.split(",");
      if (tableTypeStrings != null && tableTypeStrings.length > 0)
      {
        for (final String tableTypeString: tableTypeStrings)
        {
          tableTypes.add(tableTypeString.trim());
        }
      }
    }
    else
    {
      tableTypes = null;
    }
    return tableTypes;
  }

  /**
   * Defensively set SchemaCrawlerOptions.
   *
   * @return SchemaCrawlerOptions
   */
  private SchemaCrawlerOptions createSchemaCrawlerOptions()
  {
    final Log logger = getLog();

    final SchemaCrawlerOptions schemaCrawlerOptions = new SchemaCrawlerOptions();

    if (!isBlank(table_types))
    {
      schemaCrawlerOptions.setTableTypes(splitTableTypes(table_types));
    }

    if (!isBlank(infolevel))
    {
      try
      {
        schemaCrawlerOptions.setSchemaInfoLevel(InfoLevel.valueOf(infolevel)
          .buildSchemaInfoLevel());
      }
      catch (final Exception e)
      {
        logger.info("Unknown infolevel - using 'standard': " + infolevel);
        schemaCrawlerOptions
          .setSchemaInfoLevel(InfoLevel.standard.buildSchemaInfoLevel());
      }
    }

    schemaCrawlerOptions
      .setSchemaInclusionRule(new RegularExpressionRule(StringUtils
        .defaultString(schemas, INCLUDE_ALL), INCLUDE_NONE));
    schemaCrawlerOptions
      .setTableInclusionRule(new RegularExpressionRule(StringUtils
        .defaultString(tables, INCLUDE_ALL), INCLUDE_NONE));
    schemaCrawlerOptions
      .setRoutineInclusionRule(new RegularExpressionRule(StringUtils
        .defaultString(routines, INCLUDE_ALL), INCLUDE_NONE));

    schemaCrawlerOptions
      .setColumnInclusionRule(new RegularExpressionRule(INCLUDE_ALL, StringUtils
        .defaultString(excludecolumns, INCLUDE_NONE)));
    schemaCrawlerOptions
      .setColumnInclusionRule(new RegularExpressionRule(INCLUDE_ALL, StringUtils
        .defaultString(excludeinout, INCLUDE_NONE)));

    return schemaCrawlerOptions;
  }

  private File executeSchemaCrawler()
    throws Exception
  {
    final Log logger = getLog();

    final SchemaCrawlerOptions schemaCrawlerOptions = createSchemaCrawlerOptions();
    final ConnectionOptions connectionOptions = createConnectionOptions();
    final Config additionalConfiguration = createAdditionalConfiguration();
    final File outputFile = File.createTempFile("schemacrawler.report.",
                                                ".html");
    final OutputOptions outputOptions = createOutputOptions(outputFile
      .toPath());

    final Executable executable = new SchemaCrawlerExecutable(command);
    executable.setOutputOptions(outputOptions);
    executable.setSchemaCrawlerOptions(schemaCrawlerOptions);
    executable.setAdditionalConfiguration(additionalConfiguration);

    logger.debug(ObjectToString.toString(executable));
    executable.execute(connectionOptions.getConnection());

    return outputFile;
  }

  /**
   * The JDBC driver classpath comes from the configuration of the SchemaCrawler
   * plugin. The current classloader needs to be "fixed" to include the JDBC
   * driver in the classpath.
   *
   * @throws MavenReportException
   */
  private void fixClassPath()
    throws MavenReportException
  {

    final Log logger = getLog();

    try
    {
      final List<URL> jdbcJarUrls = new ArrayList<URL>();
      for (final Object artifact: project.getArtifacts())
      {
        jdbcJarUrls.add(((Artifact) artifact).getFile().toURI().toURL());
      }
      for (final Artifact artifact: pluginArtifacts)
      {
        jdbcJarUrls.add(artifact.getFile().toURI().toURL());
      }
      logger.debug("SchemaCrawler - Maven Plugin: classpath: " + jdbcJarUrls);

      final Method addUrlMethod = URLClassLoader.class
        .getDeclaredMethod("addURL", new Class[] {
                                                   URL.class
      });
      addUrlMethod.setAccessible(true);

      final URLClassLoader classLoader = (URLClassLoader) getClass()
        .getClassLoader();

      for (final URL jdbcJarUrl: jdbcJarUrls)
      {
        addUrlMethod.invoke(classLoader, jdbcJarUrl);
      }

      logger.info("Fixed SchemaCrawler classpath: "
                  + Arrays.asList(classLoader.getURLs()));

    }
    catch (final Exception e)
    {
      throw new MavenReportException("Error fixing classpath", e);
    }
  }

}
