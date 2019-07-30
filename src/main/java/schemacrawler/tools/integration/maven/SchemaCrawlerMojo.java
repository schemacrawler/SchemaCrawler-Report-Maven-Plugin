/*
 *
 * SchemaCrawler Report - Apache Maven Plugin
 * http://www.schemacrawler.com
 * Copyright (c) 2011-2019, Sualeh Fatehi.
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

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.util.List;
import java.util.Locale;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.doxia.sink.Sink;
import org.apache.maven.doxia.sink.SinkFactory;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.reporting.AbstractMavenReport;
import org.apache.maven.reporting.MavenReportException;
import schemacrawler.schemacrawler.*;
import schemacrawler.tools.executable.SchemaCrawlerExecutable;
import schemacrawler.tools.integration.graph.GraphOutputFormat;
import schemacrawler.tools.iosource.FileInputResource;
import schemacrawler.tools.options.OutputFormat;
import schemacrawler.tools.options.OutputOptions;
import schemacrawler.tools.options.OutputOptionsBuilder;
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
   * SchemaCrawler command.
   */
  @Parameter(property = "command", required = true)
  private String command;
  /**
   * Config file.
   */
  @Parameter(property = "config", defaultValue = "schemacrawler.config.properties", required = true)
  private String config;
  /**
   * Regular expression to match fully qualified column names, in the
   * form "CATALOGNAME.SCHEMANAME.TABLENAME.COLUMNNAME" - for example,
   * .*\.STREET|.*\.PRICE matches columns named STREET or PRICE in any
   * table Columns that match the pattern are not displayed
   */
  @Parameter(property = "excludecolumns", defaultValue = INCLUDE_NONE)
  private String excludecolumns;
  /**
   * Regular expression to match fully qualified parameter names.
   * Parameters that match the pattern are not displayed
   */
  @Parameter(property = "excludeparameters", defaultValue = INCLUDE_NONE)
  private String excludeparameters;
  /**
   * The info level determines the amount of database metadata
   * retrieved, and also determines the time taken to crawl the schema.
   */
  @Parameter(property = "infolevel", defaultValue = "standard", required = true)
  private String infolevel;
  /**
   * Whether to hide tables with no data.
   */
  @Parameter(property = "noemptytables", defaultValue = "false")
  private boolean noemptytables;
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
   * Output format.
   */
  @Parameter(property = "outputformat", defaultValue = "html")
  private String outputformat;
  /**
   * Database connection user password.
   */
  @Parameter(property = "password", defaultValue = "${schemacrawler.password}")
  private String password;
  /**
   * The commandline dependencies.
   */
  @Parameter(property = "commandline.artifacts", required = true, readonly = true)
  private List<Artifact> pluginArtifacts;
  /**
   * Whether to show portable database object names.
   */
  @Parameter(property = "portablenames", defaultValue = "false")
  private boolean portablenames;
  /**
   * Regular expression to match fully qualified routine names, in the
   * form "CATALOGNAME.SCHEMANAME.ROUTINENAME" - for example,
   * .*\.C.*|.*\.P.* matches any routines whose names start with C or P
   * Routines that do not match the pattern are not displayed
   */
  @Parameter(property = "routines", defaultValue = INCLUDE_ALL)
  private String routines;
  /**
   * Schemas to include.
   */
  @Parameter(property = "schemas", defaultValue = INCLUDE_ALL)
  private String schemas;
  /**
   * Regular expression to match fully qualified sequence names, in the
   * form "CATALOGNAME.SCHEMANAME.SEQUENCENAME" - for example,
   * .*\.C.*|.*\.P.* Sequences that do not match the pattern are not
   * displayed.
   */
  @Parameter(property = "sequences", defaultValue = INCLUDE_ALL)
  private String sequences;
  /**
   * Sort columns in a table alphabetically.
   */
  @Parameter(property = "sortcolumns", defaultValue = "false")
  private boolean sortcolumns;
  /**
   * Sort parameters in a routine alphabetically.
   */
  @Parameter(property = "sortparameters", defaultValue = "false")
  private boolean sortparameters;
  /**
   * Sort tables alphabetically.
   */
  @Parameter(property = "sorttables", defaultValue = "true")
  private boolean sorttables;
  /**
   * Regular expression to match fully qualified synonym names, in the
   * form "CATALOGNAME.SCHEMANAME.SYNONYMNAME" - for example,
   * .*\.C.*|.*\.P.* Synonyms that do not match the pattern are not
   * displayed.
   */
  @Parameter(property = "synonyms", defaultValue = INCLUDE_ALL)
  private String synonyms;
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
   * Title for the SchemaCrawler Report.
   */
  @Parameter(property = "title", defaultValue = "SchemaCrawler Report")
  private String title;
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
      throw new MavenReportException(
        "Error executing SchemaCrawler command " + command, e);
    }
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
    throw new MavenReportException(
      "Should not execute report, generate(...) is overridden");
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
   * @see org.apache.maven.reporting.MavenReport#getDescription(java.util.Locale)
   */
  @Override
  public String getDescription(final Locale locale)
  {
    return getName(locale);
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

  private DataSource createDataSource()
  {
    final BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUsername(user);
    dataSource.setPassword(password);
    dataSource.setUrl(url);
    dataSource.setDefaultAutoCommit(false);
    dataSource.setInitialSize(1);
    dataSource.setMaxTotal(1);

    return dataSource;
  }

  private OutputOptions createOutputOptions(final Path outputFile)
  {
    final OutputOptionsBuilder outputOptionsBuilder = OutputOptionsBuilder
      .builder();
    outputOptionsBuilder.withOutputFormatValue(getOutputFormat().getFormat());
    outputOptionsBuilder.withOutputFile(outputFile);
    outputOptionsBuilder.title(title);
    return outputOptionsBuilder.toOptions();
  }

  /**
   * Defensively set SchemaCrawlerOptions.
   *
   * @return SchemaCrawlerOptions
   */
  private SchemaCrawlerOptions createSchemaCrawlerOptions()
  {
    final Log logger = getLog();

    final SchemaCrawlerOptionsBuilder optionsBuilder = SchemaCrawlerOptionsBuilder
      .builder();

    optionsBuilder.tableTypes(tableTypes);

    if (!isBlank(infolevel))
    {
      try
      {
        optionsBuilder.withSchemaInfoLevel(InfoLevel.valueOf(infolevel)
                                             .toSchemaInfoLevel());
      }
      catch (final Exception e)
      {
        logger.info("Unknown infolevel - using 'standard': " + infolevel);
        optionsBuilder
          .withSchemaInfoLevel(InfoLevel.standard.toSchemaInfoLevel());
      }
    }

    optionsBuilder.includeSchemas(new RegularExpressionRule(defaultString(
      schemas,
      INCLUDE_ALL), INCLUDE_NONE));
    optionsBuilder.includeSynonyms(new RegularExpressionRule(defaultString(
      synonyms,
      INCLUDE_ALL), INCLUDE_NONE));
    optionsBuilder.includeSequences(new RegularExpressionRule(defaultString(
      sequences,
      INCLUDE_ALL), INCLUDE_NONE));
    optionsBuilder.includeTables(new RegularExpressionRule(defaultString(tables,
                                                                         INCLUDE_ALL),
                                                           INCLUDE_NONE));
    optionsBuilder.includeRoutines(new RegularExpressionRule(defaultString(
      routines,
      INCLUDE_ALL), INCLUDE_NONE));

    optionsBuilder.includeColumns(new RegularExpressionRule(INCLUDE_ALL,
                                                            defaultString(
                                                              excludecolumns,
                                                              INCLUDE_NONE)));
    optionsBuilder.includeRoutineParameters(new RegularExpressionRule(
      INCLUDE_ALL,
      defaultString(excludeparameters, INCLUDE_NONE)));

    if (noemptytables)
    {
      optionsBuilder.noEmptyTables();
    }

    return optionsBuilder.toOptions();
  }

  private Config createSchemaTextOptions()
  {
    final SchemaTextOptionsBuilder textOptionsBuilder = SchemaTextOptionsBuilder
      .builder();

    textOptionsBuilder.noInfo(noinfo);
    textOptionsBuilder.noRemarks(noremarks);
    textOptionsBuilder.portableNames(portablenames);

    textOptionsBuilder.sortTables(sorttables);
    textOptionsBuilder.sortTableColumns(sortcolumns);
    textOptionsBuilder.sortRoutineParameters(sortparameters);

    final Config textOptionsConfig = textOptionsBuilder.toConfig();
    return textOptionsConfig;
  }

  private Path executeSchemaCrawler()
    throws Exception
  {
    final Log logger = getLog();

    final SchemaCrawlerOptions schemaCrawlerOptions = createSchemaCrawlerOptions();
    final DataSource dataSource = createDataSource();
    final Config additionalConfiguration = createAdditionalConfiguration();
    final Path outputFile = Files
      .createTempFile("schemacrawler.report.", ".data");
    final OutputOptions outputOptions = createOutputOptions(outputFile);

    final SchemaCrawlerExecutable executable = new SchemaCrawlerExecutable(
      command);
    executable.setOutputOptions(outputOptions);
    executable.setSchemaCrawlerOptions(schemaCrawlerOptions);
    executable.setAdditionalConfiguration(additionalConfiguration);

    logger.debug(ObjectToString.toString(executable));
    final Connection connection = dataSource.getConnection();
    final SchemaRetrievalOptions schemaRetrievalOptions = SchemaCrawlerUtility
      .matchSchemaRetrievalOptions(connection);
    executable.setConnection(connection);
    executable.setSchemaRetrievalOptions(schemaRetrievalOptions);

    executable.execute();

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

}
