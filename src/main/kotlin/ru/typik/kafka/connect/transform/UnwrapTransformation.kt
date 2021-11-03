package ru.nspk.tpp.kafka.connect.transformations

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.transforms.Transformation
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

abstract class UnwrapTransformation<R : ConnectRecord<R>> : Transformation<R> {
    companion object {
        private val LOG = LoggerFactory.getLogger(UnwrapTransformation::class.java)
        private const val WRAPPED_FIELD_NAME = "value"
    }

    private val schemaCache: MutableMap<Schema, Schema> = ConcurrentHashMap()

    override fun configure(configs: Map<String, *>) {}

    override fun close() {}

    override fun config(): ConfigDef =
        ConfigDef()

    protected fun process(schema: Schema?, value: Any?): SchemaAndValue = when {
        schema?.type() == Schema.Type.STRUCT && value != null ->
            processStruct(schema, value as Struct)
        else ->
            SchemaAndValue(schema, value)
    }

    private fun Field.innerValueField() = schema().fields()[0]

    private fun Schema.optionalSchema(): Schema =
        when (type()) {
            Schema.Type.STRING -> Schema.OPTIONAL_STRING_SCHEMA
            else -> throw UnsupportedOperationException("Type ${type()} isn't supported")
        }

    private fun Field.isWrappedStringField(): Boolean =
        schema().type() == Schema.Type.STRUCT &&
            schema().fields().size == 1 &&
            schema().fields()[0]
                .let { valueField ->
                    valueField.name() == WRAPPED_FIELD_NAME &&
                        valueField.schema().type() in listOf(Schema.Type.STRING)
                }

    private fun Schema.toOutputSchema() = SchemaBuilder.struct()
        .name(name())
        .also { builder -> if (isOptional) builder.optional() }
        .also { builder ->
            fields().forEach { field ->
                builder.field(
                    field.name(),
                    if (field.isWrappedStringField())
                        field.innerValueField().schema().optionalSchema()
                    else
                        field.schema()
                )
            }
        }
        .build()

    private fun Struct.toOutputValue(inputSchema: Schema, outputSchema: Schema) =
        Struct(outputSchema)
            .also { output ->
                inputSchema.fields().forEach { field ->
                    when {
                        field.isWrappedStringField() -> {
                            kotlin.runCatching {
                                getStruct(field.name()).get(WRAPPED_FIELD_NAME)
                            }
                                .onSuccess { wrappedValue ->
                                    output.put(field.name(), wrappedValue)
                                }
                                .onFailure { error ->
                                    LOG.trace("Extraction wrapped value for field ${field.name()} ended with error", error)
                                }
                        }
                        else -> output.put(field.name(), get(field))
                    }
                }
            }

    private fun processStruct(inputSchema: Schema, inputValue: Struct): SchemaAndValue {
        val outputSchema = schemaCache.computeIfAbsent(inputSchema) { inputSchema.toOutputSchema() }
        val outputStruct = inputValue.toOutputValue(inputSchema, outputSchema)
        return SchemaAndValue(outputSchema, outputStruct)
    }

    class Key<R : ConnectRecord<R>> : UnwrapTransformation<R>() {
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

    class Value<R : ConnectRecord<R>> : UnwrapTransformation<R>() {
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
