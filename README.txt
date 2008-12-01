Copyright 2008 Boomi, Inc.
All rights reserved.

Basic REST server for Google AppEngine Applications using the builtin Datastore API.

========

For example client usage, see example.txt.

========

To use with an existing application:


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

