# Broker External Storage
The `mem_broker` supports external storage through HTTP API.
The external storage may support multiple `undermoon` clusters at the same time,
so there's a `<name>` argument in the path to differentiate these clusters.

Query data:
```
GET /api/v1/store/<name>
Response:
  HTTP 200: ExternalStore json
```

Update data:
```
PUT /api/v1/store/<name>
Request: ExternalStore json
Response:
  HTTP 200 for success
  HTTP 409 for version conflict
```

The structure of `ExternalStore` is:
```
{
    "version": <string> or null,
    "store": <data>
}
```

In the [undermoon-operator](https://github.com/doyoubi/undermoon-operator),
this `version` is `ResourceVersion` of kubernetes object.
The HTTP service should check the `version` before updating the data.
