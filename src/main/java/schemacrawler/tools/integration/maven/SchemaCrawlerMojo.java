/*
 *
 * SchemaCrawler Report - Apache Maven Plugin
 * http://www.schemacrawler.com
 * Copyright (c) 2011-2020, Sualeh Fatehi.
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


import static org.apache.commons.lang.StringUtils.isBlank;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import org.apache.commons.lang.StringUtils;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.doxia.sink.Sink;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.reporting.AbstractMavenReport;
import org.apache.maven.reporting.MavenReportException;
import schemacrawler.Main;

/**
 * Generates a SchemaCrawler report of the database.
 */
@Mojo(name = "schemacrawler", requiresReports = true, threadSafe = true)
public class SchemaCrawlerMojo
  extends AbstractMavenReport
{
  
  /**
   * Log level for SchemaCrawler. One of 
   * [OFF, SEVERE, WARNING, INFO, CONFIG, FINE, FINER, FINEST, ALL]
   */
  @Parameter(property = "loglevel", defaultValue = "OFF")
  private String loglevel;
  /**
   * SchemaCrawler command.
   */
  @Parameter(property = "command", required = true)
  private String command;
  /**
   * Config file.
   */
  @Parameter(property = "config",
             defaultValue = "schemacrawler.config.properties",
             required = true)
  private File config;
  /**
   * Regular expression to match fully qualified column names, in the form
   * "CATALOGNAME.SCHEMANAME.TABLENAME.COLUMNNAME" - for example,
   * .*\.STREET|.*\.PRICE matches columns named STREET or PRICE in any table
   * Columns that match the pattern are not displayed
   */
  @Parameter(property = "excludecolumns")
  private String excludecolumns;
  /**
   * Regular expression to match fully qualified parameter names. Parameters
   * that match the pattern are not displayed
   */
  @Parameter(property = "excludeparameters")
  private String excludeparameters;
  /**
   * The info level determines the amount of database metadata retrieved, and
   * also determines the time taken to crawl the schema.
   */
  @Parameter(property = "infolevel", defaultValue = "standard", required = true)
  private String infolevel;
  /**
   * The info level determines the amount of database metadata retrieved, and
   * also determines the time taken to crawl the schema.
   */
  @Parameter(property = "loadrowcounts", defaultValue = "false")
  private boolean loadrowcounts;
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
   * Output file.
   */
  @Parameter(property = "outputfile",
             defaultValue = "schemacrawler-output.html")
  private String outputfile;
  /**
   * Database connection user password.
   */
  @Parameter(property = "password", defaultValue = "${schemacrawler.password}")
  private String password;
  /**
   * The commandline dependencies.
   */
  @Parameter(property = "commandline.artifacts",
             required = true,
             readonly = true)
  private List<Artifact> pluginArtifacts;
  /**
   * Whether to show portable database object names.
   */
  @Parameter(property = "portablenames", defaultValue = "false")
  private boolean portablenames;
  /**
   * Regular expression to match fully qualified routine names, in the form
   * "CATALOGNAME.SCHEMANAME.ROUTINENAME" - for example, .*\..*\.C.*|.*\..*\.P.*
   * matches any routines whose names start with C or P Routines that do not
   * match the pattern are not displayed
   */
  @Parameter(property = "routines")
  private String routines;
  /**
   * Schemas to include.
   */
  @Parameter(property = "schemas")
  private String schemas;
  /**
   * Regular expression to match fully qualified sequence names, in the form
   * "CATALOGNAME.SCHEMANAME.SEQUENCENAME" - for example,
   * .*\..*\.C.*|.*\..*\.P.* Sequences that do not match the pattern are not
   * displayed.
   */
  @Parameter(property = "sequences")
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
   * Regular expression to match fully qualified synonym names, in the form
   * "CATALOGNAME.SCHEMANAME.SYNONYMNAME" - for example, .*\..*\.C.*|.*\..*\.P.*
   * Synonyms that do not match the pattern are not displayed.
   */
  @Parameter(property = "synonyms")
  private String synonyms;
  /**
   * Comma-separated list of table types of TABLE,VIEW,SYSTEM_TABLE,GLOBAL_TEMPORARY,LOCAL_TEMPORARY,ALIAS
   */
  @Parameter(property = "table_types")
  private String tableTypes;

  /**
   * Regular expression to match fully qualified table names, in the form
   * "CATALOGNAME.SCHEMANAME.TABLENAME" - for example, .*\..*\.C.*|.*\..*\.P.*
   * Tables that do not match the pattern are not displayed.
   */
  @Parameter(property = "tables")
  private String tables;
  /**
   * Title for the SchemaCrawler Report.
   */
  @Parameter(property = "title", defaultValue = "SchemaCrawler Report")
  private String title;
  /**
   * Database connection string.
   */
  @Parameter(property = "url",
             defaultValue = "${schemacrawler.url}",
             required = true)
  private String url;
  /**
   * Database connection user name.
   */
  @Parameter(property = "user",
             defaultValue = "${schemacrawler.user}",
             required = true)
  private String user;

  /**
   * {@inheritDoc}
   *
   * @see org.apache.maven.reporting.MavenReport#getOutputName()
   */
  @Override
  public String getOutputName()
  {
    return "schemacrawler-report";
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

  @Override
  protected void executeReport(final Locale locale)
    throws MavenReportException
  {
    try
    {
      final Path outputFile = executeSchemaCrawler();
      final Path reportFile = Paths
        .get(getOutputDirectory(), getOutputFilename())
        .toAbsolutePath();     

      Files.move(outputFile, reportFile, StandardCopyOption.REPLACE_EXISTING);
      final Log logger = getLog();
      logger.info("Generated SchemaCrawler report, " + reportFile);

      // Get the Maven Doxia Sink, which will be used to generate the
      // various elements of the document
      Sink mainSink = getSink();
      if (mainSink == null)
      {
        throw new MavenReportException("Could not get the Doxia sink");
      }

      // Page title
      mainSink.head();
      mainSink.title();
      mainSink.text(getName(Locale.getDefault()));
      mainSink.title_();
      mainSink.head_();

      mainSink.body();

      // Heading 1
      mainSink.section1();
      mainSink.sectionTitle1();
      mainSink.text(getName(Locale.getDefault()));
      mainSink.sectionTitle1_();

      // Content
      mainSink.paragraph();
      mainSink.link(getOutputFilename());
      mainSink.text(title);
      mainSink.link_();
      mainSink.paragraph_();

      // Close
      mainSink.section1_();
      mainSink.body_();
    }
    catch (final Exception e)
    {
      throw new MavenReportException("Error executing SchemaCrawler report", e);
    }
  }

  private String getOutputFilename()
  {
    if (outputfile == null || StringUtils.isBlank(outputfile))
    {
      return String.format("schemacrawler-output.%s", outputformat);
    }
    else
    {
      return Paths
        .get(outputfile)
        .getFileName()
        .toString();
    }
  }

  private Path executeSchemaCrawler() 
      throws Exception
  {

    final Map<String, String> argsMap = new HashMap<>();

    if (config != null)
    {
      final Path configPath = config.toPath().toAbsolutePath();
      argsMap.put("--config-file", configPath.toString());
    }

    // Load command
    if (!isBlank(infolevel))
    {
      argsMap.put("--info-level", infolevel);
    }
    if (loadrowcounts)
    {
      argsMap.put("--load-row-counts", Boolean.TRUE.toString());
    }

    // Limit command
    if (!isBlank(tableTypes))
    {
      argsMap.put("--table-types", tableTypes);
    }
    if (schemas != null)
    {
      argsMap.put("--schemas", schemas);
    }
    if (synonyms != null)
    {
      argsMap.put("--synonyms", synonyms);
    }
    if (sequences != null)
    {
      argsMap.put("--sequences", sequences);
    }
    if (tables != null)
    {
      argsMap.put("--tables", tables);
    }
    if (routines != null)
    {
      argsMap.put("--routines", routines);
    }
    if (excludecolumns != null)
    {
      argsMap.put("--excludecolumns", excludecolumns);
    }
    if (excludeparameters != null)
    {
      argsMap.put("--excludeparameters", excludeparameters);
    }

    // Filter command
    if (noemptytables)
    {
      argsMap.put("--no-empty-tables", Boolean.TRUE.toString());
    }

    // Show command
    if (noinfo)
    {
      argsMap.put("--no-info", Boolean.TRUE.toString());
    }
    if (noremarks)
    {
      argsMap.put("--no-remarks", Boolean.TRUE.toString());
    }
    if (portablenames)
    {
      argsMap.put("--portable-names", Boolean.TRUE.toString());
    }

    // Sort command
    if (sorttables)
    {
      argsMap.put("--sort-tables", Boolean.TRUE.toString());
    }
    if (sortcolumns)
    {
      argsMap.put("--sort-columns", Boolean.TRUE.toString());
    }
    if (sortparameters)
    {
      argsMap.put("--sort-parameters", Boolean.TRUE.toString());
    }
       
    // Connect command
    argsMap.put("--url", url);
    argsMap.put("--user", user);
    if (password != null) {
      argsMap.put("--password", password);
    }
   
    // Log command
    argsMap.put("--log-level", loglevel);
    
    // Execute command
    if (!isBlank(title))
    {
      argsMap.put("--title", title);
    }
    if (!isBlank(outputformat))
    {
      argsMap.put("--output-format", outputformat);
    }
    if (!isBlank(outputformat))
    {
      argsMap.put("--output-format", outputformat);
    }
    
    if (!isBlank(command))
    {
      argsMap.put("--command", command);
    }
    
    // Output into a temporary file
    final Path outputFile = Paths
        .get(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString())
        .toAbsolutePath().normalize();
    argsMap.put("--output-file", outputFile.toString());

    // Build command-line
    final List<String> argsList = new ArrayList<>();
    for (final Entry<String, String> entry : argsMap.entrySet())
    {
      argsList.add(entry.getKey());
      argsList.add(entry.getValue());
    }
    final String[] args = argsList.toArray(new String[0]);

    final Log logger = getLog();
    logger.info("Executing SchemaCrawler with: " + argsMap);

    // Run SchemaCrawler command-line
    Main.main(args);
    if (!Files.isReadable(outputFile)) 
    {
      throw new IOException("SchemaCrawler report was not generated");
    }
    logger.info("Completed SchemaCrawler execution");

    return outputFile;
  }

}
