package com.poc.spark.app;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;

public class JsonToAvroSchemaGenerator {

    private static final String INPUT_JSON_FILE = "/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/input.json";  // Input JSON file
    private static final String OUTPUT_AVSC_FILE = "/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/output_schema.avsc";  // Output Avro schema file

    public static void main(String[] args) {
        try {
            // Read JSON file (assuming it's a JSON array or newline-delimited JSON)
            String jsonContent = new String(Files.readAllBytes(Paths.get(INPUT_JSON_FILE)));
            ObjectMapper objectMapper = new ObjectMapper();

            // Parse first JSON object to infer schema
            JsonNode jsonNode = objectMapper.readTree(jsonContent);
            if (jsonNode.isArray() && jsonNode.size() > 0) {
                jsonNode = jsonNode.get(0);  // Use first element for schema inference
            }

            // Generate Avro schema
            Schema avroSchema = generateAvroSchema(jsonNode, "GeneratedSchema");

            // Write Avro schema to file
            try (FileWriter fileWriter = new FileWriter(OUTPUT_AVSC_FILE)) {
                fileWriter.write(avroSchema.toString(true));
            }

            System.out.println("Avro schema generated successfully: " + OUTPUT_AVSC_FILE);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Function to generate Avro schema dynamically
    private static Schema generateAvroSchema(JsonNode jsonNode, String recordName) {
        SchemaBuilder.RecordBuilder<Schema> recordBuilder = SchemaBuilder.record(recordName);
        SchemaBuilder.FieldAssembler<Schema> fields = recordBuilder.fields();

        Iterator<Map.Entry<String, JsonNode>> fieldsIterator = jsonNode.fields();
        while (fieldsIterator.hasNext()) {
            Map.Entry<String, JsonNode> field = fieldsIterator.next();
            String fieldName = field.getKey();
            JsonNode value = field.getValue();

            // Infer Avro types based on JSON value type
            if (value.isInt()) {
                fields = fields.name(fieldName).type().nullable().intType().noDefault();
            } else if (value.isLong()) {
                fields = fields.name(fieldName).type().nullable().longType().noDefault();
            } else if (value.isFloat() || value.isDouble()) {
                fields = fields.name(fieldName).type().nullable().doubleType().noDefault();
            } else if (value.isBoolean()) {
                fields = fields.name(fieldName).type().nullable().booleanType().noDefault();
            } else if (value.isArray()) {
                fields = fields.name(fieldName).type().nullable().array().items().stringType().noDefault();
            } else if (value.isObject()) {
                Schema nestedSchema = generateAvroSchema(value, fieldName + "Record");
                fields = fields.name(fieldName).type().unionOf().nullType().and().type(nestedSchema).endUnion().noDefault();
            } else {  // Default to string for other types
                fields = fields.name(fieldName).type().nullable().stringType().noDefault();
            }
        }

        return fields.endRecord();
    }
}

