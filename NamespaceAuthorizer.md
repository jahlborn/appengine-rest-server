# Overview #

The Dispatcher supports custom authorization by plugging in a custom Authorizer.  Below is an example implementation of an Authenticator controlling access to Model instances based on the currently specified namespace.  The [namespace can be set](http://code.google.com/appengine/docs/python/multitenancy/multitenancy.html#Setting_the_Current_Namespace) either globally or on a per-user basis (e.g. in a custom Authenticator).

You would utilize this Authorizer by setting it on the Dispatcher:
```
rest.Dispatcher.authorizer = NamespaceAuthorizer()
```

# NamespaceAuthorizer #

```
from google.appengine.api import namespace_manager


class NamespaceAuthorizer(rest.Authorizer):

    def can_read(self, dispatcher, model):
        if(model.key().namespace() != namespace_manager.get_namespace()):
            dispatcher.not_found()

    def filter_read(self, dispatcher, models):
        return self.filter_models(models)

    def can_write(self, dispatcher, model, is_replace):
        if(model.is_saved() and (model.key().namespace() != namespace_manager.get_namespace())):
            dispatcher.not_found()

    def filter_write(self, dispatcher, models, is_replace):
        return self.filter_models(models)

    def can_delete(self, dispatcher, model_type, model_key):
        if(model_key.namespace() != namespace_manager.get_namespace()):
            dispatcher.not_found()

    def filter_models(self, models):
        cur_ns = namespace_manager.get_namespace()
        models[:] = [model for model in models if model.key().namespace() == cur_ns]
        return models
```