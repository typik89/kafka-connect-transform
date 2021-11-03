# Introduction
This project contains common transformations with Kafka Connect.

# Installation
For Docker image confluentinc/cp-kafka-connect-base:6.2.0 build jar and place it to directory /usr/share/java/kafka-serde-tools


# Transformations
## [UnwrapTransformation]

Unwrap value from google.protobuf.StringValue(google/protobuf/wrappers.proto) struct. 
Another structs from wrappers.proto could be implemented additionally.

*Key*
```
ru.typik.kafka.connect.transform.UnwrapTransformation$Key
```
*Value*
```
ru.typik.kafka.connect.transform.UnwrapTransformation$Value
```
### Example

#### Example with value
*Input*
```
Struct{
  nullable=Struct{value=value},
  simple=36773d6e-5ee4-4900-8d91-bd051a0896c7
}
```
*Output*
```
Struct{
  nullable=value,
  simple=36773d6e-5ee4-4900-8d91-bd051a0896c7
}
```
#### Example with no value
*Input*
```
Struct{
  simple=9ed9f48f-63e0-427b-b795-30b69bbd6d55
}
```
*Output*
```
Struct{
  simple=9ed9f48f-63e0-427b-b795-30b69bbd6d55
}
```



## [JsonOrRawTransformation]

Validate value from 'source' field. 
Place the field's value to 'target.json' field if it's valid json.
Place the field's value to 'target.raw' field if it's invalid json.

*Key*
```
ru.typik.kafka.connect.transform.JsonOrRawTransformation$Key
```
*Value*
```
ru.typik.kafka.connect.transform.JsonOrRawTransformation$Value
```
### Configuration

#### General

##### `source`

The field to verify is it correct json.

*Importance:* HIGH

*Type:* STRING

*Validator:* Matches: ``LOWER_HYPHEN``, ``LOWER_UNDERSCORE``, ``LOWER_CAMEL``, ``UPPER_CAMEL``, ``UPPER_UNDERSCORE``

##### `target.json`

The field to place 'source' in case it is valid json

*Importance:* HIGH

*Type:* STRING

*Validator:* Matches: ``LOWER_HYPHEN``, ``LOWER_UNDERSCORE``, ``LOWER_CAMEL``, ``UPPER_CAMEL``, ``UPPER_UNDERSCORE``

##### `target.raw`

The field to place 'source' in case it is inValid json

*Importance:* HIGH

*Type:* STRING

*Validator:* Matches: ``LOWER_HYPHEN``, ``LOWER_UNDERSCORE``, ``LOWER_CAMEL``, ``UPPER_CAMEL``, ``UPPER_UNDERSCORE``


### Example

#### Transformation

```
{
  ...
  "transforms.jsonOrRaw.source": "requestBody",
  "transforms.jsonOrRaw.target.json": "requestBody",
  "transforms.jsonOrRaw.target.raw": "rawBody",
  ...
}
```

#### Example with valid json
*Input*
```
Struct{
  value={ "a" : "b" },
  other=other
}
```
*Output*
```
Struct{
  json={ "a" : "b" },
  other=other
}
```
#### Example with invalid json
*Input*
```
Struct{
  value=asdf,
  other=other
 }
```
*Output*
```
Struct{
  raw=asdf,
  other=other
 }
```




