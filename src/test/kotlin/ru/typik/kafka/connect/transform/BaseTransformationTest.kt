package ru.typik.kafka.connect.transform

import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord

abstract class BaseTransformationTest {

    protected fun Struct.toValueSinkRecord() = SinkRecord(
        "topic",
        1,
        null,
        null,
        this.schema(),
        this,
        1L
    )

    protected fun Struct.toKeySinkRecord() = SinkRecord(
        "topic",
        1,
        this.schema(),
        this,
        null,
        null,
        1L
    )
}
