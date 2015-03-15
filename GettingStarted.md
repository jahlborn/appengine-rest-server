# Getting Started #

## Setup ##

Utilizing this library is extremely simple.  Assuming you have the library code installed under the directory "rest" within your application (i.e. `"rest/__init__.py"`), you would add the following to your main application code:

```
import rest

# add a handler for "rest" calls
application = webapp.WSGIApplication([
  <... existing webservice urls ...>
  ('/rest/.*', rest.Dispatcher)
], ...)

# configure the rest dispatcher to know what prefix to expect on request urls
rest.Dispatcher.base_url = "/rest"

# add all models from the current module, and/or...
rest.Dispatcher.add_models_from_module(__name__)
# add all models from some other module, and/or...
rest.Dispatcher.add_models_from_module(my_model_module)
# add specific models
rest.Dispatcher.add_models({
  "foo": FooModel,
  "bar": BarModel})
# add specific models (with given names) and restrict the supported methods
rest.Dispatcher.add_models({
  'foo' : (FooModel, rest.READ_ONLY_MODEL_METHODS),
  'bar' : (BarModel, ['GET_METADATA', 'GET', 'POST', 'PUT'],
  'cache' : (CacheModel, ['GET', 'DELETE'] })

# optionally use custom authentication/authorization
rest.Dispatcher.authenticator = MyAuthenticator()
rest.Dispatcher.authorizer = MyAuthorizer()

```

For some example implementations of custom authentication and authorization see [ExampleAuthenticator](ExampleAuthenticator.md) and [ExampleAuthorizer](ExampleAuthorizer.md).

## Client Usage ##

Once this server has been installed in your application, the basic usage is as follows (assuming you installed the REST API with the url prefix "/rest" as shown above).

  * Metadata Browsing
    * `GET http://<service>/rest/metadata`
      * Gets all known types
    * `GET http://<service>/rest/metadata/<typeName>`
      * Gets the `<typeName>` type profile (as XML Schema).  (If the model is an Expando model, the schema will include an "any" element).
  * Object Manipulation
    * `GET http://<service>/rest/<typeName>`
      * Gets the first page of `<typeName>` instances (number returned per page is defined by server).  The returned list element will contain an "offset" attribute.  If it has a value, that is the next offset to use to retrieve more results.  If it is empty, there are no more results.
    * `GET http://<service>/rest/<typeName>?offset=50`
      * Gets the page of `<typeName>` instances starting at offset 50 (0 based numbering).  The offset should generally be filled in from a previous request.
    * `GET http://<service>/rest/<typeName>?<queryTerm>[&<queryTerm>]`
      * Gets a page of `<typeName>` instances using a query filter created from the given query terms (with offset features mentioned above).
        * Multiple query terms will be AND'ed together to create the filter.
        * A query filter term has the structure: `f<op>_<propertyName>=<value>`
          * Examples:
            * `"feq_author=bob@example.com"` means include instances where the value of the "author" property is equal to "bob@example.com"
            * `"flt_count=37&fin_content=value1,value2"` means include instances where the value of the "count" property greater than "37" and the value of the content property is "value1" or "value2"
        * Available operations:
          * `"feq_" -> "equal to"`
          * `"flt_" -> "less than"`
          * `"fgt_" -> "greater than"`
          * `"fle_" -> "less than or equal to"`
          * `"fge_" -> "greater than or equal to"`
          * `"fne_" -> "not equal to"`
          * `"fin_" -> "in <commaSeparatedList>"`
        * Blob and Text properties may not be used in a query filter
    * `GET http://<service>/rest/<typeName>/<key>`
      * Gets the single `<typeName>` instance with the given `<key>`
    * `POST http://<service>/rest/<typeName>`
      * Create new `<typeName>` instance using the posted data which should adhere to the XML Schema for the type
      * Returns the key of the new instance by default.  With "?type=full" at the end of the url, returns the entire updated instance like a GET request.
    * `POST http://<service>/rest/<typeName>/<key>`
      * Partial update of the existing `<typeName>` instance with the given `<key>`.  Will only modify fields included in the posted xml data.
      * (Returns same as previous request)
    * `PUT http://<service>/rest/<typeName>/<key>`
      * Complete replacement of the existing `<typeName>` instance with the given `<key>`
      * (Returns same as previous request)
    * `DELETE http://<service>/rest/<typeName>/<key>`
      * Delete the existing `<typeName>` instance