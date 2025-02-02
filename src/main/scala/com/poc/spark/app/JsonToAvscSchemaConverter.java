package com.poc.spark.app;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class JsonToAvscSchemaConverter {

    public static void main(String[] args) {

        String jsonFilePath = "/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/json-to-avsc-sample-input.json";

        try {
            // Read JSON file and convert to Avro schema
            Schema avroSchema = convertJsonFileToAvroSchema(jsonFilePath, "GeneratedRecord");

            // Print the Avro Schema
            System.out.println("Generated Avro Schema:\n" + avroSchema.toString(true));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Schema convertJsonFileToAvroSchema(String jsonFilePath, String recordName) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonArrayNode = objectMapper.readTree(new File(jsonFilePath));

        // Ensure it's a JSON array
        if (!jsonArrayNode.isArray() || jsonArrayNode.size() == 0) {
            throw new IllegalArgumentException("JSON file must contain a non-empty array.");
        }

        // Use the first object in the array to generate the schema
        JsonNode firstObject = jsonArrayNode.get(0);

        // Start building Avro Schema
        SchemaBuilder.RecordBuilder<Schema> recordBuilder = SchemaBuilder.record(recordName);
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = recordBuilder.fields();

        // Iterate through JSON fields
        Iterator<Map.Entry<String, JsonNode>> fields = firstObject.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String fieldName = field.getKey();
            JsonNode fieldValue = field.getValue();

            // Determine Avro type
            Schema fieldSchema = determineAvroType(fieldName, fieldValue);
            fieldAssembler.name(fieldName).type(fieldSchema).noDefault();
        }

        // Wrap schema inside an array type
        return Schema.createArray(fieldAssembler.endRecord());
    }

    private static Schema determineAvroType(String fieldName, JsonNode value) {
        if (value.isInt()) {
            return Schema.create(Schema.Type.INT);
        } else if (value.isLong()) {
            return Schema.create(Schema.Type.LONG);
        } else if (value.isDouble() || value.isFloat()) {
            return Schema.create(Schema.Type.DOUBLE);
        } else if (value.isBoolean()) {
            return Schema.create(Schema.Type.BOOLEAN);
        } else if (value.isTextual()) {
            return Schema.create(Schema.Type.STRING);
        } else if (value.isArray()) {
            return Schema.createArray(Schema.create(Schema.Type.STRING)); // Default to string array
        } else if (value.isObject()) {
            // Handle nested object by creating a new record schema
            SchemaBuilder.RecordBuilder<Schema> nestedRecord = SchemaBuilder.record(fieldName);
            SchemaBuilder.FieldAssembler<Schema> nestedFields = nestedRecord.fields();

            Iterator<Map.Entry<String, JsonNode>> subFields = value.fields();
            while (subFields.hasNext()) {
                Map.Entry<String, JsonNode> subField = subFields.next();
                String subFieldName = subField.getKey();
                JsonNode subFieldValue = subField.getValue();
                nestedFields.name(subFieldName).type(determineAvroType(subFieldName, subFieldValue)).noDefault();
            }

            return nestedFields.endRecord();
        } else {
            return Schema.create(Schema.Type.NULL);
        }
    }
}
