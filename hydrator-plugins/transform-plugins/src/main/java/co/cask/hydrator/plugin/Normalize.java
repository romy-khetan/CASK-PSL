/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.hydrator.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Transforms records by normalizing the data.
 * Convert wide rows and reducing data to it canonicalize form
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("Normalize")
@Description("Convert wide rows and reducing data to it canonicalize form")
public class Normalize extends Transform<StructuredRecord, StructuredRecord> {
  private final NormalizeConfig config;

  private Schema outputSchema;
  private Map<String, String> inputOutputFieldMap;
  private Map<String, String> normalizeFieldMap;
  private List<String> normalizeFieldList;

  public Normalize(NormalizeConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(getOutputSchema());
  }

  private Schema getOutputSchema() {
    //create output schema
    List<Schema.Field> outputFields = Lists.newArrayList();
    inputOutputFieldMap = new HashMap<String, String>();
    String[] fieldMappingArray = config.fieldMapping.split(",");
    for (String fieldMapping : fieldMappingArray) {
      String[] mappings = fieldMapping.split(":");
      Preconditions.checkArgument(mappings.length == 2, "Input and output schema fields mapping is invalid.");
      inputOutputFieldMap.put(mappings[0], mappings[1]);
      outputFields.add(Schema.Field.of(mappings[1], Schema.of(Schema.Type.STRING)));
    }

    normalizeFieldMap = new HashMap<String, String>();
    normalizeFieldList = Lists.newArrayList();
    String[] fieldNormalizingArray = config.fieldNormalizing.split(",");
    for (String fieldNormalizing : fieldNormalizingArray) {
      String[] normalizing = fieldNormalizing.split(":");
      Preconditions.checkArgument(normalizing.length == 3, "Normalizing fields mapping is invalid.");
      normalizeFieldList.add(normalizing[0]);
      normalizeFieldMap.put(normalizing[0] + "_name", normalizing[1]);
      normalizeFieldMap.put(normalizing[0] + "_value", normalizing[2]);
      outputFields.add(Schema.Field.of(normalizing[1], Schema.of(Schema.Type.STRING)));
      outputFields.add(Schema.Field.of(normalizing[2], Schema.of(Schema.Type.STRING)));
    }
    return Schema.recordOf("outputSchema", outputFields);
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    if (outputSchema == null) {
      outputSchema = getOutputSchema();
    }
  }

  @Override
  public void transform(StructuredRecord structuredRecord, Emitter<StructuredRecord> emitter) throws Exception {
    for (String normalizeField : normalizeFieldList) {
      StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
      builder.set(normalizeFieldMap.get(normalizeField + "_name"), normalizeField)
        .set(normalizeFieldMap.get(normalizeField + "_value"), structuredRecord.get(normalizeField));

      Set<String> keySet = inputOutputFieldMap.keySet();
      Iterator<String>  itr = keySet.iterator();
      while (itr.hasNext()) {
        String field = itr.next();
        builder.set(inputOutputFieldMap.get(field), structuredRecord.get(field));
      }
      emitter.emit(builder.build());
    }
  }
  /**
   * Configuration for the Normalize transform.
   */
  public static class NormalizeConfig extends PluginConfig {
    @Description("Specify the input schema field mapping to output schema field. " +
      "Example: CustomerID:ID, here value of CustomerID will be saved to ID field of output schema.")
    private final String fieldMapping;

    @Description("Specify the normalize field name, to what output field it should be mapped to and where the value " +
      "needs to be added. Example: ItemId:AttributeType:AttributeValue, here ItemId text will be saved to " +
      "AttributeType field and its value will be saved to AttributeValue field of output schema.")
    private final String fieldNormalizing;

    public NormalizeConfig(String fieldMapping, String fieldNormalizing) {
      this.fieldMapping = fieldMapping;
      this.fieldNormalizing = fieldNormalizing;
    }
  }
}
