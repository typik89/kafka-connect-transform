package ru.typik.kafka.connect.transform

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EmptySource
import org.junit.jupiter.params.provider.ValueSource

internal class JsonOrRawTransformationTest : BaseTransformationTest() {

    private val inputSchema = SchemaBuilder.struct()
        .field("value", Schema.OPTIONAL_STRING_SCHEMA)
        .field("other", Schema.STRING_SCHEMA)
        .build()

    private fun String.toInputStruct() = Struct(inputSchema)
        .put("value", this)
        .put("other", "$this-other")

    private val keyTransformation = JsonOrRawTransformation.Key<SinkRecord>()
    private val valueTransformation = JsonOrRawTransformation.Value<SinkRecord>()

    init {
        listOf(keyTransformation, valueTransformation).forEach { transformation ->
            transformation.configure(
                mapOf(
                    JsonOrRawTransformation.SOURCE_FIELD_CONF to "value",
                    JsonOrRawTransformation.TARGET_JSON_FIELD_CONF to "json",
                    JsonOrRawTransformation.TARGET_RAW_FIELD_CONF to "raw"
                )
            )
        }
    }

    @ParameterizedTest
    @EmptySource
    @ValueSource(strings = ["asdf"])
    fun `place to raw field if invalid json`(invalidJson: String) {
        val input = invalidJson.toInputStruct()

        val transformedKeyRecord: SinkRecord = keyTransformation.apply(input.toKeySinkRecord())
        val transformedValueRecord: SinkRecord = valueTransformation.apply(input.toValueSinkRecord())

        assertNotNull(transformedKeyRecord)
        assertNotNull(transformedValueRecord)

        listOf(transformedKeyRecord.keySchema(), transformedValueRecord.valueSchema())
            .forEach { schema -> schema.assertInputSchema() }

        listOf(transformedKeyRecord.key(), transformedValueRecord.value())
            .forEach { value ->
                assertNotNull(value as Struct)
                assertNull(value.getString("json"))
                assertEquals("$invalidJson-other", value.getString("other"))
                assertEquals(invalidJson, value.getString("raw"))
            }
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            """{ "a" : "b" }"""
        ]
    )
    fun `place to json field if valid json`(validJson: String) {
        val input = validJson.toInputStruct()

        val transformedKeyRecord: SinkRecord = keyTransformation.apply(input.toKeySinkRecord())
        val transformedValueRecord: SinkRecord = valueTransformation.apply(input.toValueSinkRecord())

        assertNotNull(transformedKeyRecord)
        assertNotNull(transformedValueRecord)

        listOf(transformedKeyRecord.keySchema(), transformedValueRecord.valueSchema())
            .forEach { schema -> schema.assertInputSchema() }

        listOf(transformedKeyRecord.key(), transformedValueRecord.value())
            .forEach { value ->
                assertNotNull(value as Struct)
                assertEquals(validJson, value.getString("json"))
                assertEquals("$validJson-other", value.getString("other"))
                assertNull(value.getString("raw"))
            }
    }

    private fun Schema.assertInputSchema() {
        assertEquals(Schema.Type.STRUCT, type())
        assertEquals(3, fields().size)
        assertEquals("json", fields()[0].name())
        assertEquals(Schema.Type.STRING, fields()[0].schema().type())
        assertEquals("raw", fields()[1].name())
        assertEquals(Schema.Type.STRING, fields()[1].schema().type())
        assertEquals("other", fields()[2].name())
        assertEquals(Schema.Type.STRING, fields()[2].schema().type())
    }
}
