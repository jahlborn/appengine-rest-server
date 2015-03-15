# Overview #

The Dispatcher supports custom authorization by plugging in a custom Authorizer.  Below is an example implementation of an Authenticator controlling access to Model instances based on a UserProperty field named "owner" on every Model class.

You would utilize this Authorizer by setting it on the Dispatcher:
```
rest.Dispatcher.authorizer = OwnerAuthorizer()
```

# OwnerAuthorizer #

```
from google.appengine.api import users


class OwnerAuthorizer(rest.Authorizer):

    def can_read(self, dispatcher, model):
        if(model.owner != users.get_current_user()):
            dispatcher.not_found()

    def filter_read(self, dispatcher, models):
        return self.filter_models(models)

    def check_query(self, dispatcher, query_expr, query_params):
        query_params.append(users.get_current_user())
        if(not query_expr):
            query_expr = 'WHERE owner = :%d' % (len(query_params))
        else:
            query_expr += ' AND owner = :%d' % (len(query_params))
        return query_expr

    def can_write(self, dispatcher, model, is_replace):
        if(not model.is_saved()):
            # creating a new model
            model.owner = users.get_current_user()
        elif(model.owner != users.get_current_user()):
            dispatcher.not_found()

    def filter_write(self, dispatcher, models, is_replace):
        return self.filter_models(models)

    def can_delete(self, dispatcher, model_type, model_key):
        query = model_type.all(True).filter("owner = ", users.get_current_user()).filter("__key__ = ", model_key)
        if(len(query.fetch(1)) == 0):
            dispatcher.not_found()

    def filter_models(self, models):
        cur_user = users.get_current_user()
        models[:] = [model for model in models if model.owner == cur_user]
        return models
```