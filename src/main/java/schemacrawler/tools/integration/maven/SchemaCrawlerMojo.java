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


import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

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
import schemacrawler.schemacrawler.InclusionRule;
import schemacrawler.schemacrawler.SchemaCrawlerException;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.tools.executable.Executable;
import schemacrawler.tools.executable.SchemaCrawlerExecutable;
import schemacrawler.tools.options.InfoLevel;
import schemacrawler.tools.options.OutputFormat;
import schemacrawler.tools.options.OutputOptions;
import sf.util.ObjectToString;
import sf.util.Utility;

/**
 * Generates a SchemaCrawler report of the database.
 */
@Mojo(name="schemacrawler", requiresReports=true, threadSafe=true)
public class SchemaCrawlerMojo
  extends AbstractMavenReport
{
  @Component
  private MavenProject project;

  /**
   * Config file.
   */
  @Parameter(property="config", defaultValue="config.properties", required=true)
  private String config;

  /**
   * Config override file.
   */
  @Parameter(property="config-override", defaultValue="schemacrawler.config.override.properties")
  private String configOverride;

  /**
   * JDBC driver class name.
   */
  @Parameter(property="driver", defaultValue="${schemacrawler.driver}", required=true)
  private String driver;

  /**
   * Database connection string.
   */
  @Parameter(property="url", defaultValue="${schemacrawler.url}", required=true)
  private String url;

  /**
   * Database connection user name.
   */
  @Parameter(property="user", defaultValue="${schemacrawler.user}", required=true)
  private String user;

  /**
   * Database connection user password.
   */
  @Parameter(property="password", defaultValue="${schemacrawler.password}")
  private String password;

  /**
   * Command.
   */
  @Parameter(property="command", required=true)
  private String command;

  /**
   * The plugin dependencies.
   */
  @Parameter(property="plugin.artifacts", required=true, readonly=true)
  private List<Artifact> pluginArtifacts;

  /**
   * Sort tables alphabetically.
   */
  @Parameter(property="sorttables", defaultValue="true")
  private boolean sorttables;

  /**
   * Sort columns in a table alphabetically.
   */
  @Parameter(property="sortcolumns", defaultValue="false")
  private boolean sortcolumns;

  /**
   * Sort parameters in a stored procedure alphabetically.
   */
  @Parameter(property="sortinout", defaultValue="false")
  private boolean sortinout;

  /**
   * The info level determines the amount of database metadata
   * retrieved, and also determines the time taken to crawl the schema.
   */
  @Parameter(property="infolevel", defaultValue="standard", required=true)
  private final InfoLevel infolevel = InfoLevel.standard;

  /**
   * Schemas to include.
   */
  @Parameter(property="schemas", defaultValue=InclusionRule.ALL)
  private final String schemas = InclusionRule.ALL;

  /**
   * Comma-separated list of table types of
   * TABLE,VIEW,SYSTEM_TABLE,GLOBAL_TEMPORARY,LOCAL_TEMPORARY,ALIAS
   */
  @Parameter(property="table_types")
  private String table_types;

  /**
   * Regular expression to match fully qualified table names, in the
   * form "CATALOGNAME.SCHEMANAME.TABLENAME" - for example,
   * .*\.C.*|.*\.P.* Tables that do not match the pattern are not
   * displayed.
   */
  @Parameter(property="tables", defaultValue=InclusionRule.ALL)
  private final String tables = InclusionRule.ALL;

  /**
   * Regular expression to match fully qualified column names, in the
   * form "CATALOGNAME.SCHEMANAME.TABLENAME.COLUMNNAME" - for example,
   * .*\.STREET|.*\.PRICE matches columns named STREET or PRICE in any
   * table Columns that match the pattern are not displayed
   */
  @Parameter(property="excludecolumns", defaultValue=InclusionRule.NONE)
  private final String excludecolumns = InclusionRule.NONE;

  /**
   * Regular expression to match fully qualified procedure names, in the
   * form "CATALOGNAME.SCHEMANAME.PROCEDURENAME" - for example,
   * .*\.C.*|.*\.P.* matches any procedures whose names start with C or
   * P Procedures that do not match the pattern are not displayed
   */
  @Parameter(property="procedures", defaultValue=InclusionRule.ALL)
  private final String procedures = InclusionRule.ALL;

  /**
   * Regular expression to match fully qualified parameter names.
   * Parameters that match the pattern are not displayed
   */
  @Parameter(property="excludeinout", defaultValue=InclusionRule.NONE)
  private final String excludeinout = InclusionRule.NONE;

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
      sink.rawText(Utility.readFully(new FileReader(outputFile)));

      sink.flush();
    }
    catch (final Exception e)
    {
      throw new MavenReportException("Error executing SchemaCrawler command " + command, e);
    }
  }

  /**
   * @see org.apache.maven.reporting.AbstractMavenReport#getOutputDirectory()
   */
  @Override
  protected String getOutputDirectory()
  {
    return null; // Unused in the Maven API
  }

  /**
   * @see org.apache.maven.reporting.AbstractMavenReport#getProject()
   */
  @Override
  protected MavenProject getProject()
  {
    return project;
  }

  /**
   * @see org.apache.maven.reporting.AbstractMavenReport#getSiteRenderer()
   */
  @Override
  protected Renderer getSiteRenderer()
  {
    return null; // Unused in the Maven API
  }

  private OutputOptions createOutputOptions(final File outputFile)
  {
    final OutputOptions outputOptions = new OutputOptions();
    outputOptions.setOutputFormatValue(OutputFormat.html.name());
    outputOptions.setAppendOutput(false);
    outputOptions.setNoHeader(true);
    outputOptions.setNoFooter(true);
    outputOptions.setOutputFileName(outputFile.getAbsolutePath());
    return outputOptions;
  }

  private SchemaCrawlerOptions createSchemaCrawlerOptions()
  {
    final SchemaCrawlerOptions schemaCrawlerOptions = new SchemaCrawlerOptions();
    if (!Utility.isBlank(table_types))
    {
      schemaCrawlerOptions.setTableTypes(table_types);
    }
    schemaCrawlerOptions.setAlphabeticalSortForTables(sorttables);
    schemaCrawlerOptions.setAlphabeticalSortForTableColumns(sortcolumns);
    schemaCrawlerOptions.setAlphabeticalSortForProcedureColumns(sortinout);
    schemaCrawlerOptions.setSchemaInfoLevel(infolevel.getSchemaInfoLevel());
    schemaCrawlerOptions
      .setSchemaInclusionRule(new InclusionRule(schemas, InclusionRule.NONE));
    schemaCrawlerOptions
      .setTableInclusionRule(new InclusionRule(tables, InclusionRule.NONE));
    schemaCrawlerOptions
      .setProcedureInclusionRule(new InclusionRule(procedures,
                                                   InclusionRule.NONE));
    schemaCrawlerOptions
      .setColumnInclusionRule(new InclusionRule(InclusionRule.ALL,
                                                excludecolumns));
    schemaCrawlerOptions
      .setProcedureColumnInclusionRule(new InclusionRule(InclusionRule.ALL,
                                                         excludeinout));
    return schemaCrawlerOptions;
  }

  private File executeSchemaCrawler()
    throws IOException, SchemaCrawlerException, Exception, SQLException
  {
    final Log logger = getLog();

    final File outputFile = File.createTempFile("schemacrawler.report.",
                                                ".html");
    final Config additionalConfiguration = Config.load(config, configOverride);

    // Execute SchemaCrawler
    final Executable executable = new SchemaCrawlerExecutable(command);
    executable.setOutputOptions(createOutputOptions(outputFile));
    executable.setSchemaCrawlerOptions(createSchemaCrawlerOptions());
    executable.setAdditionalConfiguration(additionalConfiguration);

    final ConnectionOptions connectionOptions = new DatabaseConnectionOptions(driver,
                                                                              url);
    connectionOptions.setUser(user);
    connectionOptions.setPassword(password);

    logger.debug(ObjectToString.toString(executable));
    executable.execute(connectionOptions.getConnection());
    return outputFile;
  }

  /**
   * The JDBC driver classpath comes from the configuration of the
   * SchemaCrawler plugin. The current classloader needs to be "fixed"
   * to include the JDBC driver in the classpath.
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
