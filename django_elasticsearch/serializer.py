from django.utils.importlib import import_module
from datetime import datetime, date, time
#TODO Add content type cache
from utils import ModelLazyObject
from json import JSONDecoder, JSONEncoder
import uuid

class Decoder(JSONDecoder):
    """Extends the base simplejson JSONDecoder for Dejavu."""
    def __init__(self, arena=None, encoding=None, object_hook=None, **kwargs):
        JSONDecoder.__init__(self, encoding, object_hook, **kwargs)
        if not self.object_hook:
            self.object_hook = self.json_to_python
        self.arena = arena

    def json_to_python(self, son):
        
        if isinstance(son, dict):
            if "_type" in son and son["_type"] in [u"django", u'emb']:
                son = self.decode_django(son)
            else:
                for (key, value) in son.items():
                    if isinstance(value, dict):
                        if "_type" in value and value["_type"] in [u"django", u'emb']:
                            son[key] = self.decode_django(value)
                        else:
                            son[key] = self.json_to_python(value)
                    elif hasattr(value, "__iter__"): # Make sure we recurse into sub-docs
                        son[key] = [self.json_to_python(item) for item in value]
                    else: # Again, make sure to recurse into sub-docs
                        son[key] = self.json_to_python(value)
        elif hasattr(son, "__iter__"): # Make sure we recurse into sub-docs
            son = [self.json_to_python(item) for item in son]
        return son

    def decode_django(self, data):
        from django.contrib.contenttypes.models import ContentType
        if data['_type']=="django":
            model = ContentType.objects.get(app_label=data['_app'], model=data['_model'])
            return ModelLazyObject(model.model_class(), data['pk'])
        elif data['_type']=="emb":
            try:
                model = ContentType.objects.get(app_label=data['_app'], model=data['_model']).model_class()
            except:
                module = import_module(data['_app'])
                model = getattr(module, data['_model'])            
            
            del data['_type']
            del data['_app']
            del data['_model']
            data.pop('_id', None)
            values = {}
            for k,v in data.items():
                values[str(k)] = self.json_to_python(v)
            return model(**values)

class Encoder(JSONEncoder):
    def __init__(self, *args, **kwargs):
        JSONEncoder.__init__(self, *args, **kwargs)
        

    def encode_django(self, model):
        """
        Encode ricorsive embedded models and django models
        """
        from django_elasticsearch.fields import EmbeddedModel
        if isinstance(model, EmbeddedModel):
            if model.pk is None:
                model.pk = str(uuid.uuid4())
            res = {'_app':model._meta.app_label, 
                   '_model':model._meta.module_name,
                   '_id':model.pk}
            for field in model._meta.fields:
                res[field.attname] = self.default(getattr(model, field.attname))
            res["_type"] = "emb"
            from django.contrib.contenttypes.models import ContentType
            try:
                ContentType.objects.get(app_label=res['_app'], model=res['_model'])
            except:
                res['_app'] = model.__class__.__module__
                res['_model'] = model._meta.object_name
                
            return res
        if not model.pk:
            model.save()
        return {'_app':model._meta.app_label, 
                '_model':model._meta.module_name,
                'pk':model.pk,
                '_type':"django"}

    def default(self, value):
        """Convert rogue and mysterious data types.
        Conversion notes:
        
        - ``datetime.date`` and ``datetime.datetime`` objects are
        converted into datetime strings.
        """
        from django.db.models import Model
        from django_elasticsearch.fields import EmbeddedModel

        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%dT%H:%M:%S")
        elif isinstance(value, date):
            dt = datetime(value.year, value.month, value.day, 0, 0, 0)
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
#        elif isinstance(value, dict):
#            for (key, value) in value.items():
#                if isinstance(value, (str, unicode)):
#                    continue
#                if isinstance(value, (Model, EmbeddedModel)):
#                    value[key] = self.encode_django(value, collection)
#                elif isinstance(value, dict): # Make sure we recurse into sub-docs
#                    value[key] = self.transform_incoming(value)
#                elif hasattr(value, "__iter__"): # Make sure we recurse into sub-docs
#                    value[key] = [self.transform_incoming(item) for item in value]
        elif isinstance(value, (str, unicode)):
            pass
        elif hasattr(value, "__iter__"): # Make sure we recurse into sub-docs
            value = [self.transform_incoming(item, collection) for item in value]
        elif isinstance(value, (Model, EmbeddedModel)):
            value = self.encode_django(value)
        return value
