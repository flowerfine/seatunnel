# Klaviyo

> Klaviyo source connector
 
## Description

Used to read data from Klaviyo.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

##  Options

| name                        | type   | required | default value |
| --------------------------- | ------ | -------- | ------------- |
| url                         | String | Yes      | -             |
| private_key                 | String | Yes      | -             |
| revision                    | String | Yes      | -             |
| method                      | String | No       | get           |
| schema                      | Config | No       | -             |
| schema.fields               | Config | No       | -             |
| format                      | String | No       | json          |
| params                      | Map    | No       | -             |
| body                        | String | No       | -             |
| poll_interval_ms            | int    | No       | -             |
| retry                       | int    | No       | -             |
| retry_backoff_multiplier_ms | int    | No       | 100           |
| retry_backoff_max_ms        | int    | No       | 10000         |
| common-options              | config | No       | -             |

### url [String]

http request url

### private_key [String]

API private key for login, you can get more detail at this link:

https://developers.klaviyo.com/en/docs/retrieve_api_credentials

### revision [String]

API endpoint revision (format: YYYY-MM-DD)

### method [String]

http request method, only supports GET, POST method

### params [Map]

http params

### body [String]

http body

### poll_interval_ms [int]

request http api interval(millis) in stream mode

### retry [int]

The max retry times if request http return to `IOException`

### retry_backoff_multiplier_ms [int]

The retry-backoff times(millis) multiplier if request http failed

### retry_backoff_max_ms [int]

The maximum retry-backoff times(millis) if request http failed

### format [String]

the format of upstream data, now only support `json` `text`, default `json`.

when you assign format is `json`, you should also assign schema option, for example:

upstream data is the following:

```json
{"code":  200, "data":  "get success", "success":  true}
```

you should assign schema as the following:

```hocon
schema {
    fields {
        code = int
        data = string
        success = boolean
    }
}
```

connector will generate data as the following:

| code | data        | success |
|------|-------------|---------|
| 200  | get success | true    |

when you assign format is `text`, connector will do nothing for upstream data, for example:

upstream data is the following:

```json
{"code":  200, "data":  "get success", "success":  true}
```

connector will generate data as the following:

| content |
|---------|
| {"code":  200, "data":  "get success", "success":  true}        |

### schema [Config]

#### fields [Config]

the schema fields of upstream data

### common options

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details

## Example

```hocon
Klaviyo {
    url = "https://a.klaviyo.com/api/lists/"
    private_key = "Seatunnel-test"
    revision = "2020-10-17"
    method = "GET"
    format = "json"
    schema = {
          fields {
            type = string
            id = string
            attributes = {
                  name = string
                  created = string
                  updated = string
            }
            links = {
                  self = string
            }
          }
    }
}
```

## Changelog

### next version

- Add Klaviyo Source Connector