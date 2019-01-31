# Elasticsearch translate Ingest Processor

This processor replace value using `dictionary`. It likes [Logstash translate filter plugin](https://www.elastic.co/guide/en/logstash/current/plugins-filters-translate.html#plugins-filters-translate)

Now, this processor only supports String field in original document and in dictionary.

## Usage


```
PUT _ingest/pipeline/translate-pipeline
{
"description" : "_description",
"processors" : [
  {
    "translate" : {
      "field" : "source_field",
      "target_field" : "target_field",
      "dictionary" : {
        "10" : "success",
        "20" : "fail"
      }
    }
  }
]
}

PUT /my-index/my-type/1?pipeline=translate-pipeline
{
  "source_field" : ["10", "20"]
}

GET /my-index/my-type/1
{
  "source_field" : ["10", "20"]
  "target_field": ["success", "fail"]
}
```

## Configuration

| Parameter | Use | Required |
| --- | --- | --- |
| field   | The name of the field containing the value to be compared for a match by the translate processor | Yes |
| target_field  | The field you wish to populate with the translated code. | Yes |
| dictionary  | The dictionary configuration item can contain a hash representing the mapping. | Yes |
| ignore_missing  | If `true`, the document doesn't have a `field`, rase an error | No |
| default  | Processor sets this value if the value of `field` isn't in `dictinary`. If no `default`, the processor set `null` | No |

## Setup

In order to install this plugin, you need to create a zip distribution first by running

```bash
gradle clean check
```

This will produce a zip file in `build/distributions`.

After building the zip file, you can install it like this

```bash
bin/elasticsearch-plugin install file:///path/to/ingest-translate/build/distribution/ingest-translate-0.0.1-SNAPSHOT.zip
```

## Bugs & TODO

* There are always bugs
* and todos...

