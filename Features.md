# Features #

The appengine rest server has a wide range of features for interacting with Models.

## Basic Features ##
  * Metadata browsing
    * List models: `GET "/metadata"`
    * Define model in XML Schema: `GET "/metadata/MyModel"`
  * Reading data (XML output)
    * Get specific instance: `GET "/MyModel/<key>"`
    * Get all instances (paged results): `GET "/MyModel"`
      * Results can be ordered using the "ordering" query param: `"?ordering=<propertyName>"`
    * Find multiple instances (paged results): `GET "/MyModel?<queryParams>"`
      * Results can be ordered (same as get all)
  * Modifying data (XML input)
    * Create new instance (returns key): `POST "/MyModel"`
      * Supports batch create by surrounding multiple instances with `<list>` element (returns keys)
    * Partial update instance (returns key): `POST "/MyModel/<key>"`
      * Supports batch update (same as create)
    * Complete update instance (returns key): `PUT "/MyModel/<key>"`
      * Supports batch update (sames as create)
    * Output of all update operations may be altered with "type" query param
      * Return the keys in an XML format: `"?type=structured"`
      * Return the entire updated models: `"?type=full"`
    * Delete instance: `"DELETE "/MyModel/<key"`

## Advanced Features ##
  * Working with individual properties
    * Read single property: `GET "/MyModel/<key>/<propertyName>"`
      * Output "Content-Type" header can be specified using input "Accept" header
    * Read single element of list property: `GET "/MyModel/<key>/<propertyName>/<index>"`
    * Write single property: `POST "/MyModel/<key>/<propertyName>"`
      * Same output options as instance updates
    * Write single element of list property: `POST "/MyModel/<key>/<propertyName>/<index>"`
    * All blob types are _not_ Base64 encoded
  * JSON support
    * Specify JSON input using header: `"Content-Type: application/json"`
    * Request JSON output using header: `"Accept: application/json"`
    * [JSONP](http://en.wikipedia.org/wiki/JSON#JSONP) support using "callback" query parameter: `"?callback=my_method"`
  * Simulate HTTP PUT/DELETE using POST with header `"X-HTTP-Method-Override: <realMethod>"`
  * Custom Authentication and Authorization (for multi-tenancy)
  * Filter returned fields using "include\_props" query parameter: `"?include_props=<prop1>,<prop2>,..."`
  * Extended BlobInfo support
    * Include extra info (as attributes) in BlobInfo reference property (creation, filename, etc.) using "blobinfo" query parameter: `"?blobinfo=info"`
    * Download actual BlobInfo data: `GET "/MyModel/<key>/<blobProperty>/content"`
    * Upload BlobInfo
      1. `POST "/MyModel/<key>/<blobProperty>/content` -> returns upload form
      1. `POST "<formUrl>"` (with actual data) -> redirect url
      1. `GET "<redirectUrl>"` -> normal update results
  * Optional ETags Support (as of the 1.0.7 release)
    * Must be enabled on the server using the `Dispatcher.enable_etags` property
    * For GET requests, an ETag header will be returned on the request which applies to the _entire_ response body.  additionally, the model elements themselves will now include an "etag" attribute which is specific to that model only.  so, for a single model retrieval, the header and model value will be the same.  for a query operation with multiple models, the header will be an aggregate value different from each model value.  GET requests will honor the "If-None-Match" header, which can either specify a single value (for single model GETs or an entire collection) or multiple values (for each individual model in a collection response).  if the "If-None-Match" header is matched, an http 304 response code will be returned.
    * For PUT/POST/DELETE requests, the "If-Match" header will be honored.  Like the "If-None-Match" header, this can be a single value or multiple values.  alternatively, the model specific etag values can be provided in the input models themselves (in an etag attribute).  If the input etags do not match, an http 412 response code will be returned and _no_ modifications will be made on the server side.  Additionally, PUT/POST responses will include updated etag information similar to GET requests.