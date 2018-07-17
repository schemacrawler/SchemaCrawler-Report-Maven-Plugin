/*
 *
 * SchemaCrawler Report - Apache Maven Plugin
 * http://www.schemacrawler.com
 * Copyright (c) 2011-2018, Sualeh Fatehi.
 *
 * This library is free software; you can redistribute it and/or modify it under the terms
 * of the GNU Lesser General Public License as published by the Free Software Foundation;
 * either version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with this
 * library; if not, write to the Free Software Foundation, Inc., 59 Temple Place, Suite 330,
 * Boston, MA 02111-1307, USA.
 *
 */
package schemacrawler.tools.integration.maven;


import static org.apache.commons.lang.StringUtils.defaultString;
import static org.apache.commons.lang.StringUtils.isBlank;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.doxia.sink.Sink;
import org.apache.maven.doxia.sink.SinkFactory;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.reporting.AbstractMavenReport;
import org.apache.maven.reporting.MavenReportException;

import schemacrawler.schemacrawler.Config;
import schemacrawler.schemacrawler.ConnectionOptions;
import schemacrawler.schemacrawler.DatabaseConnectionOptions;
import schemacrawler.schemacrawler.DatabaseSpecificOverrideOptions;
import schemacrawler.schemacrawler.RegularExpressionRule;
import schemacrawler.schemacrawler.SchemaCrawlerOptions;
import schemacrawler.schemacrawler.SingleUseUserCredentials;
import schemacrawler.schemacrawler.UserCredentials;
import schemacrawler.tools.executable.Executable;
import schemacrawler.tools.executable.SchemaCrawlerExecutable;
import schemacrawler.tools.integration.graph.GraphOutputFormat;
import schemacrawler.tools.iosource.FileInputResource;
import schemacrawler.tools.options.InfoLevel;
import schemacrawler.tools.options.OutputFormat;
import schemacrawler.tools.options.OutputOptions;
import schemacrawler.tools.options.TextOutputFormat;
import schemacrawler.tools.text.schema.SchemaTextOptionsBuilder;
import schemacrawler.utility.PropertiesUtility;
import schemacrawler.utility.SchemaCrawlerUtility;
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

  /**
   * Config file.
   */
  @Parameter(property = "config", defaultValue = "schemacrawler.config.properties", required = true)
  private String config;

  /**
   * Database connection string.
   */
  @Parameter(property = "url", defaultValue = "${schemacrawler.url}", required = true)
  private String url;

  /**
   * Database connection user name.
   */
  @Parameter(property = "user", defaultValue = "${schemacrawler.user}", required = true)
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
   * Regular expression to match fully qualified synonym names, in the
   * form "CATALOGNAME.SCHEMANAME.SYNONYMNAME" - for example,
   * .*\.C.*|.*\.P.* Synonyms that do not match the pattern are not
   * displayed.
   */
  @Parameter(property = "synonyms", defaultValue = INCLUDE_ALL)
  private String synonyms;

  /**
   * Regular expression to match fully qualified sequence names, in the
   * form "CATALOGNAME.SCHEMANAME.SEQUENCENAME" - for example,
   * .*\.C.*|.*\.P.* Sequences that do not match the pattern are not
   * displayed.
   */
  @Parameter(property = "sequences", defaultValue = INCLUDE_ALL)
  private String sequences;

  /**
   * Whether to hide tables with no data.
   */
  @Parameter(property = "hideemptytables", defaultValue = "false")
  private boolean hideemptytables;

  /**
   * Title for the SchemaCrawler Report.
   */
  @Parameter(property = "title", defaultValue = "SchemaCrawler Report")
  private String title;

  /**
   * Whether to hide additional database information.
   */
  @Parameter(property = "noinfo", defaultValue = "true")
  private boolean noinfo;

  /**
   * Whether to show table and column remarks.
   */
  @Parameter(property = "noremarks", defaultValue = "false")
  private boolean noremarks;

  /**
   * Whether to show portable database object names.
   */
  @Parameter(property = "portablenames", defaultValue = "false")
  private boolean portablenames;

  /**
   * Output format.
   */
  @Parameter(property = "outputformat", defaultValue = "html")
  private String outputformat;

  /**
   * Sort parameters in a routine alphabetically.
   */
  @Parameter(property = "sortinout", defaultValue = "false")
  private boolean sortinout;

  /**
   * The info level determines the amount of database metadata
   * retrieved, and also determines the time taken to crawl the schema.
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
  private String tableTypes;

  /**
   * Regular expression to match fully qualified table names, in the
   * form "CATALOGNAME.SCHEMANAME.TABLENAME" - for example,
   * .*\.C.*|.*\.P.* Tables that do not match the pattern are not
   * displayed.
   */
  @Parameter(property = "tables", defaultValue = INCLUDE_ALL)
  private String tables;

  /**
   * Regular expression to match fully qualified column names, in the
   * form "CATALOGNAME.SCHEMANAME.TABLENAME.COLUMNNAME" - for example,
   * .*\.STREET|.*\.PRICE matches columns named STREET or PRICE in any
   * table Columns that match the pattern are not displayed
   */
  @Parameter(property = "excludecolumns", defaultValue = INCLUDE_NONE)
  private String excludecolumns;

  /**
   * Regular expression to match fully qualified routine names, in the
   * form "CATALOGNAME.SCHEMANAME.ROUTINENAME" - for example,
   * .*\.C.*|.*\.P.* matches any routines whose names start with C or P
   * Routines that do not match the pattern are not displayed
   */
  @Parameter(property = "routines", defaultValue = INCLUDE_ALL)
  private String routines;

  /**
   * Regular expression to match fully qualified parameter names.
   * Parameters that match the pattern are not displayed
   */
  @Parameter(property = "excludeinout", defaultValue = INCLUDE_NONE)
  private String excludeinout;

  @Override
  public void generate(final Sink sink,
                       final SinkFactory sinkFactory,
                       final Locale locale)
    throws MavenReportException
  {
    try
    {
      final Path outputFile = executeSchemaCrawler();
      final Path reportFile = Paths
        .get(getOutputDirectory(), getOutputName() + ".html").toAbsolutePath();

      Files.move(outputFile, reportFile, StandardCopyOption.REPLACE_EXISTING);
    }
    catch (final Exception e)
    {
      throw new MavenReportException("Error executing SchemaCrawler command "
                                     + command,
                                     e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.maven.reporting.MavenReport#getDescription(java.util.Locale)
   */
  @Override
  public String getDescription(final Locale locale)
  {
    return getName(locale);
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
  public boolean isExternalReport()
  {
    return true;
  }

  @Override
  protected void executeReport(final Locale locale)
    throws MavenReportException
  {
    throw new MavenReportException("Should not execute report, generate(...) is overridden");
  }

  /**
   * Load configuration files, and add in other configuration options.
   *
   * @return SchemaCrawler command configuration
   */
  private Config createAdditionalConfiguration()
  {
    final Config textOptionsConfig = createSchemaTextOptions();
    try
    {
      final Config additionalConfiguration = PropertiesUtility
        .loadConfig(new FileInputResource(Paths.get(config)));
      additionalConfiguration.putAll(textOptionsConfig);
      return additionalConfiguration;
    }
    catch (final IOException e)
    {
      final Log logger = getLog();
      logger.debug(e.getMessage());

      return textOptionsConfig;
    }
  }

  private ConnectionOptions createConnectionOptions()
  {
    final UserCredentials userCredentials = new SingleUseUserCredentials(user,
                                                                         password);

    final Map<String, String> properties = new HashMap<>();
    properties.put("url", url);

    final ConnectionOptions connectionOptions = new DatabaseConnectionOptions(userCredentials,
                                                                              properties);
    return connectionOptions;
  }

  private OutputOptions createOutputOptions(final Path outputFile)
  {
    final OutputOptions outputOptions = new OutputOptions();
    outputOptions.setOutputFormatValue(getOutputFormat().getFormat());
    outputOptions.setOutputFile(outputFile);
    return outputOptions;
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

    if (!isBlank(tableTypes))
    {
      schemaCrawlerOptions.setTableTypes(splitTableTypes(tableTypes));
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
      .setSchemaInclusionRule(new RegularExpressionRule(defaultString(schemas,
                                                                      INCLUDE_ALL),
                                                        INCLUDE_NONE));
    schemaCrawlerOptions
      .setSynonymInclusionRule(new RegularExpressionRule(defaultString(synonyms,
                                                                       INCLUDE_ALL),
                                                         INCLUDE_NONE));
    schemaCrawlerOptions
      .setSequenceInclusionRule(new RegularExpressionRule(defaultString(sequences,
                                                                        INCLUDE_ALL),
                                                          INCLUDE_NONE));
    schemaCrawlerOptions
      .setTableInclusionRule(new RegularExpressionRule(defaultString(tables,
                                                                     INCLUDE_ALL),
                                                       INCLUDE_NONE));
    schemaCrawlerOptions
      .setRoutineInclusionRule(new RegularExpressionRule(defaultString(routines,
                                                                       INCLUDE_ALL),
                                                         INCLUDE_NONE));

    schemaCrawlerOptions
      .setColumnInclusionRule(new RegularExpressionRule(INCLUDE_ALL,
                                                        defaultString(excludecolumns,
                                                                      INCLUDE_NONE)));
    schemaCrawlerOptions
      .setColumnInclusionRule(new RegularExpressionRule(INCLUDE_ALL,
                                                        defaultString(excludeinout,
                                                                      INCLUDE_NONE)));

    schemaCrawlerOptions.setHideEmptyTables(hideemptytables);
    schemaCrawlerOptions.setTitle(title);

    return schemaCrawlerOptions;
  }

  private Config createSchemaTextOptions()
  {
    final SchemaTextOptionsBuilder textOptionsBuilder = new SchemaTextOptionsBuilder();

    if (noinfo)
    {
      textOptionsBuilder.noInfo();
    }
    textOptionsBuilder.noRemarks(noremarks);
    textOptionsBuilder.portableNames(portablenames);

    textOptionsBuilder.sortTables(sorttables);
    textOptionsBuilder.sortTableColumns(sortcolumns);
    textOptionsBuilder.sortInOut(sortinout);

    final Config textOptionsConfig = textOptionsBuilder.toConfig();
    return textOptionsConfig;
  }

  private Path executeSchemaCrawler()
    throws Exception
  {
    final Log logger = getLog();

    final SchemaCrawlerOptions schemaCrawlerOptions = createSchemaCrawlerOptions();
    final ConnectionOptions connectionOptions = createConnectionOptions();
    final Config additionalConfiguration = createAdditionalConfiguration();
    final Path outputFile = Files.createTempFile("schemacrawler.report.",
                                                 ".data");
    final OutputOptions outputOptions = createOutputOptions(outputFile);

    final Executable executable = new SchemaCrawlerExecutable(command);
    executable.setOutputOptions(outputOptions);
    executable.setSchemaCrawlerOptions(schemaCrawlerOptions);
    executable.setAdditionalConfiguration(additionalConfiguration);

    logger.debug(ObjectToString.toString(executable));
    final Connection connection = connectionOptions.getConnection();
    final DatabaseSpecificOverrideOptions databaseSpecificOverrideOptions = SchemaCrawlerUtility
      .matchDatabaseSpecificOverrideOptions(connection);
    executable.execute(connection, databaseSpecificOverrideOptions);

    return outputFile;
  }

  private OutputFormat getOutputFormat()
  {
    final OutputFormat outputFormat;
    if (isBlank(outputformat))
    {
      outputFormat = TextOutputFormat.html;
    }
    else if (TextOutputFormat.isSupportedFormat(outputformat))
    {
      outputFormat = TextOutputFormat.fromFormat(outputformat);
    }
    else if (GraphOutputFormat.isSupportedFormat(outputformat))
    {
      outputFormat = GraphOutputFormat.fromFormat(outputformat);
    }
    else
    {
      outputFormat = TextOutputFormat.html;
    }
    return outputFormat;
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

}
