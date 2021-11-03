package ru.typik.kafka.connect.transform

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.transforms.Transformation
import java.util.concurrent.ConcurrentHashMap

abstract class JsonOrRawTransformation<R : ConnectRecord<R>> : Transformation<R> {

    companion object {
        private val OBJECT_MAPPER = ObjectMapper()

        const val SOURCE_FIELD_CONF = "source"
        const val TARGET_JSON_FIELD_CONF = "target.json"
        const val TARGET_RAW_FIELD_CONF = "target.raw"

        private const val SOURCE_FIELD_DOC = "The field to verify is it correct json."
        private const val TARGET_JSON_FIELD_DOC = "The field to place '$SOURCE_FIELD_CONF' in case it is valid json"
        private const val TARGET_RAW_FIELD_DOC = "The field to place '$SOURCE_FIELD_CONF' in case it is not valid json"
    }

    private var sourceField: String? = null
    private var targetJsonField: String? = null
    private var targetRawField: String? = null

    override fun configure(configs: Map<String, *>) {
        val config = AbstractConfig(config(), configs)
        sourceField = config.getString(SOURCE_FIELD_CONF)
        targetJsonField = config.getString(TARGET_JSON_FIELD_CONF)
        targetRawField = config.getString(TARGET_RAW_FIELD_CONF)
    }

    override fun close() {}

    override fun config(): ConfigDef =
        ConfigDef()
            .define(SOURCE_FIELD_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, SOURCE_FIELD_DOC)
            .define(TARGET_JSON_FIELD_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, TARGET_JSON_FIELD_DOC)
            .define(TARGET_RAW_FIELD_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, TARGET_RAW_FIELD_DOC)

    private val schemaCache: MutableMap<Schema, Schema> = ConcurrentHashMap()

    private fun String.isValidJson(): Boolean =
        isNotEmpty() &&
            kotlin.runCatching {
                OBJECT_MAPPER.readTree(this)
                true
            }
                .getOrElse { false }

    private fun Schema.toTarget() =
        SchemaBuilder.struct()
            .name(name())
            .also { target -> if (isOptional) target.optional() }
            .also { target ->
                fields().forEach { field ->
                    when (field.name()) {
                        sourceField -> {
                            // Create 2 target fields instead of source field
                            target.field(targetJsonField, Schema.OPTIONAL_STRING_SCHEMA)
                            target.field(targetRawField, Schema.OPTIONAL_STRING_SCHEMA)
                        }
                        else -> target.field(field.name(), field.schema())
                    }
                }
            }
            .build()

    private fun Struct.toStruct(inputSchema: Schema, outputSchema: Schema) =
        Struct(outputSchema)
            .also { target ->
                inputSchema.fields()
                    .forEach { field ->
                        when (field.name()) {
                            sourceField -> {
                                val fieldValue = getString(sourceField)
                                target.put(
                                    if (fieldValue.isValidJson())
                                        targetJsonField
                                    else
                                        targetRawField,
                                    fieldValue
                                )
                            }
                            else -> target.put(field.name(), get(field))
                        }
                    }
            }

    private fun processStruct(inputSchema: Schema, value: Struct): SchemaAndValue {
        val outputSchema = schemaCache.computeIfAbsent(inputSchema) { inputSchema.toTarget() }
        val outputStruct = value.toStruct(inputSchema, outputSchema)
        return SchemaAndValue(outputSchema, outputStruct)
    }

    protected fun process(schema: Schema?, value: Any?): SchemaAndValue = when {
        schema?.type() == Schema.Type.STRUCT && value != null ->
            processStruct(schema, value as Struct)
        else ->
            SchemaAndValue(schema, value)
    }

    class Key<R : ConnectRecord<R>> : JsonOrRawTransformation<R>() {
        override fun apply(record: R): R =
            process(record.keySchema(), record.key())
                .run {
                    record.newRecord(
                        record.topic(),
                        record.kafkaPartition(),
                        schema(),
                        value(),
                        record.valueSchema(),
                        record.value(),
                        record.timestamp()
                    )
                }
    }

    class Value<R : ConnectRecord<R>> : JsonOrRawTransformation<R>() {
        override fun apply(record: R): R =
            process(record.valueSchema(), record.value())
                .run {
                    record.newRecord(
                        record.topic(),
                        record.kafkaPartition(),
                        record.keySchema(),
                        record.key(),
                        schema(),
                        value(),
                        record.timestamp()
                    )
                }
    }
}
