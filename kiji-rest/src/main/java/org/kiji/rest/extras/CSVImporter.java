/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kiji.rest.extras;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sun.jersey.api.representation.Form;
import com.yammer.metrics.annotation.Timed;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.HFileLoader;
import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.MapReduceJobOutput;
import org.kiji.mapreduce.bulkimport.KijiBulkImportJobBuilder;
import org.kiji.mapreduce.input.MapReduceJobInputs;
import org.kiji.mapreduce.lib.bulkimport.CSVBulkImporter;
import org.kiji.mapreduce.lib.util.CSVParser;
import org.kiji.mapreduce.tools.framework.MapReduceJobOutputFactory;
import org.kiji.rest.KijiClient;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.avro.BloomType;
import org.kiji.schema.avro.CellSchema;
import org.kiji.schema.avro.ColumnDesc;
import org.kiji.schema.avro.ComponentType;
import org.kiji.schema.avro.CompressionType;
import org.kiji.schema.avro.FamilyDesc;
import org.kiji.schema.avro.HashSpec;
import org.kiji.schema.avro.LocalityGroupDesc;
import org.kiji.schema.avro.RowKeyComponent;
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.avro.SchemaStorage;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.util.ResourceUtils;

/**
 * This REST resource is a CSV uploader
 */
@ApiAudience.Public
@Path("/e1/csvimporter")
public class CSVImporter {
  private static final Logger LOG = LoggerFactory.getLogger(CSVImporter.class);

  private static final String TABLE_LAYOUT_VERSION = "layout-1.2";
  private static final String CSV_DIR = "/tmp/csv/";
  private static final Integer NUM_EXAMPLES = 5;
  private static final String TABLE_NAME_KEY = "tablename";
  private static final String FILE_NAME_KEY = "filename";

  public static enum AvroType {
    STRING("string"),
    INTEGER("int"),
    LONG("long"),
    FLOAT("float"),
    DOUBLE("double");

    String avroType;

    AvroType(String avroType) {
      this.avroType = avroType;
    }

    String getAvroType() {
      return this.avroType;
    }

    String toCellSchema() {
      return "\"" + this.avroType + "\"";
    }

    public static AvroType fromCellSchema(String cellSchema) {
      for(AvroType type : AvroType.values()) {
        if(type.toCellSchema().equals(cellSchema)) {
          return type;
        }
      }
      return null;
    }

    public static String toDropdown(String name, AvroType defaultType) {
      StringBuilder dropdown = new StringBuilder();
      dropdown.append("<select name=\"");
      dropdown.append(name);
      dropdown.append("\">\n");

      for(AvroType type : values()) {
        dropdown.append("<option value=\"");
        dropdown.append(type.name());
        if (type == defaultType) {
          dropdown.append("\" selected>");
        } else {
          dropdown.append("\">");
        }
        dropdown.append(type.getAvroType());
        dropdown.append("</option>");
      }
      dropdown.append("\n</select>\n");
      return dropdown.toString();
    }
  }

  private final KijiClient mKijiClient;
  private final String UPLOAD_DIR = "/tmp";
  private final String FORM_HEADER = "<form action=\"csvimporter\" enctype=\"multipart/form-data\" name=\"csvimporter\" method=\"post\">\n";
  private final String FORM_FOOTER = "<input type=\"submit\" value=\"Submit\"></form>";
  private final String SIDE_DATA_INSTANCE = "side_data";

  /**
   * Default constructor.
   *
   * @param kijiClient that this should use for connecting to Kiji.
   */
  public CSVImporter(KijiClient kijiClient) {
    mKijiClient = kijiClient;
  }

  @GET
  @Timed
  @Produces(MediaType.TEXT_HTML)
  public String parse(@QueryParam("file") String file) throws Exception {
    if(file == null) {
      return "null passed in";
    }
    StringBuilder sb = new StringBuilder();
    sb.append("<p>");
    sb.append("This page assumes that you've somehow already uploaded files to /tmp.  ");
    sb.append("Because maven dependencies suck, I couldn't get multipart form uploads to work.  ");
    sb.append("So getting a real uploader is ultimately an exercise for the reader.");
    sb.append("</p>");

    sb.append(FORM_HEADER);
    sb.append("<hidden name=\"filename\" value=\"" + file + "a\">\n");
    sb.append("<p>Filename to use: ");
    sb.append("<input type=\"text\" name=\"filename\" value=\"" + file + "\">\n");

    sb.append("<p>Table name to create: ");
    String tablename = file;
    tablename = tablename.substring(0, tablename.lastIndexOf(".")).replaceAll("-", "_");
    sb.append("<input type=\"text\" name=\"tablename\" value=\"" + tablename + "\">\n");

    BufferedReader br = null;

    try {
      br = new BufferedReader(new FileReader(CSV_DIR + file));
      String line = br.readLine();
      List<String> headers = CSVParser.parseCSV(line);

      // Sample data;
      Map<String, List<String>> data = Maps.newHashMap();
      int readLines = 0;
      while ((line = br.readLine()) != null && readLines < NUM_EXAMPLES) {
        List<String> fields = CSVParser.parseCSV(line);
        for (int c = 0; c < fields.size(); c++) {
          String column = headers.get(c);
          String field = fields.get(c);
          if(!data.containsKey(column)) {
            data.put(column, new ArrayList<String>());
          }
          data.get(column).add(field);
        }
        readLines++;
      }

      sb.append("<table border=\"1\">\n");
      sb.append("<th>Column Name</th><th>Type:</th><th colspan=" + NUM_EXAMPLES + ">Examples:</th>\n");

      for(String header : headers) {
        sb.append("<tr><td>");
        sb.append(header);
        sb.append("</td><td>");
        sb.append(AvroType.toDropdown(header, guessType(data.get(header))));
        for(String exampleData : data.get(header)) {
          sb.append("</td><td>");
          sb.append(exampleData);
        }
        sb.append("</td></tr>");
      }
      sb.append("</table>");

    } finally {
      if (br != null) {
        br.close();
      }
    }
    sb.append(FORM_FOOTER);

    return sb.toString();
  }

  @POST
  @Timed
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.TEXT_HTML)
  public String processFormInput(Form form) throws Exception {
    StringBuilder sb = new StringBuilder();
    String tablename = null;

    for(Map.Entry<String, List<String>> entry : form.entrySet()) {
      Map<String, String> formFields = tokenizeForm(entry.getValue().get(0));
      String file = formFields.get(FILE_NAME_KEY);
      tablename = formFields.get(TABLE_NAME_KEY);
      sb.append("<p>\n");
      sb.append("Table " + tablename + " descriptor: " );
      TableLayoutDesc layoutDesc = toLayout(formFields);
      sb.append(layoutDesc.toString());
      sb.append("\n</p>");

      Kiji sideKiji =  mKijiClient.getKiji(SIDE_DATA_INSTANCE);
      if (sideKiji.getTableNames().contains(tablename)) {
        ResourceUtils.releaseOrLog(sideKiji);
        return "<p>Table already exists: " + tablename + "</p><p>ABORTED</p>";
      }
      sideKiji.createTable(layoutDesc);
      ResourceUtils.releaseOrLog(sideKiji);

      List<ColumnDesc> columnDescs = layoutDesc.getLocalityGroups().get(0).getFamilies().get(0).getColumns();
      Map<String, AvroType> conversionMap = Maps.newHashMap();
      for(ColumnDesc column : columnDescs) {
        AvroType type = AvroType.fromCellSchema(column.getColumnSchema().getValue());
        if(type != AvroType.STRING) {
          conversionMap.put(column.getName(), type);
        }
      }

      BufferedReader br = null;

      KijiTable table = mKijiClient.getKijiTable(SIDE_DATA_INSTANCE, tablename);
      KijiTableWriter tableWriter = table.openTableWriter();
      try {
        br = new BufferedReader(new FileReader(CSV_DIR + file));
        String line = br.readLine();
        List<String> headers = CSVParser.parseCSV(line);

        Map<String, List<String>> data = Maps.newHashMap();
        // GHETTO bulk importer
        while ((line = br.readLine()) != null) {
          List<String> fields = CSVParser.parseCSV(line);
          EntityId eid = table.getEntityId(fields.get(0));
          for (int c = 0; c < fields.size(); c++) {
            String column = headers.get(c);
            String field = fields.get(c);
            if(null==field || field.isEmpty()) {
              // Do nothing is the easiest!
            } else if(conversionMap.get(column)==AvroType.INTEGER) {
              tableWriter.put(eid, "info", column, System.currentTimeMillis(), Integer.parseInt(field));
            } else if(conversionMap.get(column) == AvroType.LONG) {
              tableWriter.put(eid, "info", column, System.currentTimeMillis(), Long.parseLong(field));
            } else if (conversionMap.get(column) == AvroType.FLOAT) {
              tableWriter.put(eid, "info", column, System.currentTimeMillis(), Float.parseFloat(field));
            } else if (conversionMap.get(column) == AvroType.DOUBLE) {
              tableWriter.put(eid, "info", column, System.currentTimeMillis(), Double.parseDouble(field));
            } else {
              // This is a string
              tableWriter.put(eid, "info", column, System.currentTimeMillis(), field);
            }
          }
        }
      } finally {
        if (br != null) {
          br.close();
        }
        ResourceUtils.closeOrLog(tableWriter);
        ResourceUtils.releaseOrLog(table);
      }

      sb.append("\n");
    }

    // Table link
    sb.append("<p>");
    sb.append("<a href=\"" + "/v1/instances/" + SIDE_DATA_INSTANCE + "/tables/" + tablename + "\">Table link</a>");
    sb.append("</p>");

    // Rows link
    sb.append("<p>");
    sb.append("<a href=\"" + "/v1/instances/" + SIDE_DATA_INSTANCE + "/tables/" + tablename + "/rows\">Rows link</a>");
    sb.append("</p>");

    sb.append("<p>Done!</p>");
    return sb.toString();
  }

  public static AvroType guessType(List<String> exampleData) {
    boolean blank = true;
    for(String data : exampleData) {
      if(!data.isEmpty()) {
        blank = false;
      }
    }
    if(blank==true) {
      // Couldn't find anything so pessimistically assume string.
      return AvroType.STRING;
    }
    try {
      for (String data : exampleData) {
        if (!data.isEmpty()) {
          Long.parseLong(data);
        }
      }
      return AvroType.LONG;
    } catch (NumberFormatException ne) {
      // Do nothing
    }

    try {
      for (String data : exampleData) {
        if (!data.isEmpty()) {
          Double.parseDouble(data);
        }
      }
      return AvroType.DOUBLE;
    } catch (NumberFormatException ne) {
      // Do nothing
    }
    return AvroType.STRING;
  }

  public static Map<String, String> tokenizeForm(String formData) {
    //FIXME this is super hacky but it's close enough
    Map<String, String> result = Maps.newHashMap();
    String[] tokens = formData.split("\"");
    for(int c=1; c<tokens.length; c+=2) {
      String key = tokens[c];
      String rawvalue = tokens[c+1].trim();
      int endPos = rawvalue.indexOf("------");
      String value = rawvalue.substring(0, endPos).trim();
      result.put(key, value);
    }
    return result;
  }

  public static TableLayoutDesc toLayout(Map<String, String> formfields) {
    TableLayoutDesc.Builder tb = TableLayoutDesc.newBuilder();
    tb.setVersion(TABLE_LAYOUT_VERSION);
    tb.setKeysFormat(makeHashPrefixedRowKeyFormat());
    //tb.setMaxFilesize(Long.MAX_VALUE);
    //tb.setMemstoreFlushsize(Long.MAX_VALUE);

    List<ColumnDesc> columns = Lists.newArrayList();
    for(Map.Entry<String, String> entry : formfields.entrySet()) {
      if(TABLE_NAME_KEY.equals(entry.getKey())) {
        tb.setName(entry.getValue());
      } else if (FILE_NAME_KEY.equals(entry.getKey())) {
        // Skip
      } else {
        // Column Description
        AvroType columnType = AvroType.valueOf(entry.getValue());
        ColumnDesc column = ColumnDesc.newBuilder()
            .setName(entry.getKey())
            .setColumnSchema(CellSchema.newBuilder()
                .setStorage(SchemaStorage.UID)
                .setType(SchemaType.INLINE)
                .setValue(columnType.toCellSchema())
                .build())
            .build();
        columns.add(column);
      }
    }
    FamilyDesc familyDesc = FamilyDesc.newBuilder()
        .setName("info")
        .setColumns(columns)
        .build();
    LocalityGroupDesc localityGroupDesc = LocalityGroupDesc.newBuilder()
        .setName("default")
        .setFamilies(Collections.singletonList(familyDesc))
        .setInMemory(false)
        .setMaxVersions(Integer.MAX_VALUE)
        .setTtlSeconds(Integer.MAX_VALUE)
        .setBloomType(BloomType.NONE)
        .setCompressionType(CompressionType.NONE)
        .build();
    tb.setLocalityGroups(Collections.singletonList(localityGroupDesc));
    return tb.build();
  }

  private static RowKeyFormat2 makeHashPrefixedRowKeyFormat() {
    // components of the row key
    ArrayList<RowKeyComponent> components = new ArrayList<RowKeyComponent>();
    components.add(RowKeyComponent.newBuilder()
        .setName("NAME").setType(ComponentType.STRING).build());

    // build the row key format
    RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().build())
        .setComponents(components)
        .build();

    return format;
  }

  //FIXME unused this is hard!
  private boolean bulkImport(String filename, String tableName) throws IOException, InterruptedException, ClassNotFoundException {
    KijiTable mOutputTable = mKijiClient.getKijiTable(SIDE_DATA_INSTANCE, tableName);
    Configuration mConf = mKijiClient.getKiji(SIDE_DATA_INSTANCE).getConf();
    //mConf.set("fs.defaultFS", "hdfs://localhost:8020");
    //FIXME mConf.set(DescribedInputTextBulkImporter.CONF_FILE, mImportDescriptorPath.toString());

    //FileSystem mFS = FileSystem.get(mConf);
    //org.apache.hadoop.fs.Path basePath = new org.apache.hadoop.fs.Path("hdfs://localhost:8020/tmp/");
    //org.apache.hadoop.fs.Path localData = new org.apache.hadoop.fs.Path(CSV_DIR + filename);
    //mFS.copyFromLocalFile(localData, basePath);

    LOG.info("Fuck it, calling exec() LIKE A CHAMPION to drop files into HDFS");
    Runtime.getRuntime().exec("hadoop fs -copyFromLocal " + CSV_DIR + filename + " /tmp");
    org.apache.hadoop.fs.Path mBulkImportInputPath = new org.apache.hadoop.fs.Path("/tmp/" + filename);
    org.apache.hadoop.fs.Path hfileDirPath = new org.apache.hadoop.fs.Path("hdfs://localhost:8020/tmp/" + filename + ".hfile");

    KijiBulkImportJobBuilder builder = KijiBulkImportJobBuilder.create();
    builder.withConf(mConf);
    builder.withBulkImporter(CSVBulkImporter.class);
    builder.withInput(MapReduceJobInputs.newTextMapReduceJobInput(mBulkImportInputPath));

    final MapReduceJobOutput output =
        MapReduceJobOutputFactory.create().fromSpaceSeparatedMap("format=hfile table=" + mOutputTable.getURI() + " nsplits=1 file=hdfs://localhost:8020/" + filename + ".hfile");
    //builder.withOutput(MapReduceJobOutputs.newHFileMapReduceJobOutput(mOutputTable.getURI(), hfileDirPath, 1));
    builder.withOutput(output);
    final KijiMapReduceJob mrjob = builder.build();

    if(mrjob.run() != false) {
      final HFileLoader loader = HFileLoader.create(mConf);
      // There is only one reducer, hence one HFile shard:
      final org.apache.hadoop.fs.Path hfilePath = new org.apache.hadoop.fs.Path(hfileDirPath, "part-r-00000.hfile");
      //FIXME at some point when this job runs we can try to load it
      loader.load(hfilePath, mOutputTable);
      return true;
    }
    return false;
  }
}
