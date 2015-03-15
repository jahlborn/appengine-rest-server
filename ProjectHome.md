# Overview #

Drop-in server for [Google App Engine](http://code.google.com/appengine/) applications which exposes your data model via a REST
API with no extra work.

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

Check out the [Features](Features.md) page for a more complete list of the features supported by the appengine rest server, including advanced features.

## Example ##

The [Boomi Demo](http://boomi-demo.appspot.com/) App Engine application is a fully working example application (based on the Google App Engine Greeting demo application).

  * Available types: http://boomi-demo.appspot.com/rest/metadata
  * Greeting schema: http://boomi-demo.appspot.com/rest/metadata/Greeting
  * Greeting instances: http://boomi-demo.appspot.com/rest/Greeting
  * Greeting instances with filter: http://boomi-demo.appspot.com/rest/Greeting?feq_author=bob@example.com&feq_date=2008-11-03T00:21:19.080553
  * Data accessible in both XML and JSON format (input can be specified using the HTTP "Content-Type" header, output can be specified using the HTTP "Accept" request header, e.g. "application/xml" or "application/json")

## Setup ##

Utilizing this library is extremely simple.  See the [Getting Started](GettingStarted.md) page to find out how to integrate the appengine rest server into your project.