# Introduction
This project contains common transformations with Kafka Connect.

# Installation
For Docker image confluentinc/cp-kafka-connect-base:6.2.0 build jar and place it to directory /usr/share/java/kafka-serde-tools


# Transformations
## [UnwrapTransformation]
*Key*
```
ru.typik.kafka.connect.transform.UnwrapTransformation$Key
```
*Value*
```
ru.typik.kafka.connect.transform.UnwrapTransformation$Value
```


## [JsonOrRawTransformation]

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


