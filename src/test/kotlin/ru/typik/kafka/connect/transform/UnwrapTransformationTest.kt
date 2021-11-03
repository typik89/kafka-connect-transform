package ru.typik.kafka.connect.transform

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.NullSource
import org.junit.jupiter.params.provider.ValueSource
import org.junit.jupiter.params.shadow.com.univocity.parsers.annotations.NullString
import ru.nspk.tpp.kafka.connect.transformations.UnwrapTransformation
import java.util.UUID

internal class UnwrapTransformationTest : BaseTransformationTest() {

    private val keyTransformation = UnwrapTransformation.Key<SinkRecord>()
    private val valueTransformation = UnwrapTransformation.Value<SinkRecord>()

    private val stringValueSchema = SchemaBuilder.struct().field("value", Schema.STRING_SCHEMA)
    private val inputSchema = SchemaBuilder.struct()
        .name("Input")
        .field("nullable", stringValueSchema)
        .field("simple", Schema.STRING_SCHEMA)
        .build()

    private fun createValueStruct(nullable: String?, simple: String) =
        Struct(inputSchema)
            .apply {
                if (nullable != null)
                    put(
                        "nullable",
                        Struct(stringValueSchema).put("value", nullable)
                    )
            }
            .put("simple", simple)

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = ["value"])
    fun test(nullableStringValue: String?) {
        val generatedStringValue = UUID.randomUUID().toString()
        val input = createValueStruct(nullableStringValue, generatedStringValue)

        val transformedKeyRecord: SinkRecord = keyTransformation.apply(input.toKeySinkRecord())
        val transformedValueRecord: SinkRecord = valueTransformation.apply(input.toValueSinkRecord())

        assertNotNull(transformedKeyRecord)
        assertNotNull(transformedValueRecord)

        listOf(transformedKeyRecord.keySchema(), transformedValueRecord.valueSchema()).forEach { schema ->
            assertEquals(Schema.Type.STRUCT, schema.type())
            assertEquals(2, schema.fields().size)
            assertEquals("nullable", schema.fields()[0].name())
            assertEquals(Schema.Type.STRING, schema.fields()[0].schema().type())
            assertEquals("simple", schema.fields()[1].name())
            assertEquals(Schema.Type.STRING, schema.fields()[1].schema().type())
        }

        listOf(transformedKeyRecord.key(), transformedValueRecord.value()).forEach { value ->
            assertNotNull(value as Struct)
            assertEquals(nullableStringValue, value.getString("nullable"))
            assertEquals(generatedStringValue, value.getString("simple"))
        }
    }
}
