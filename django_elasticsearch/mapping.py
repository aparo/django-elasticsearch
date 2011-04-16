#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyes import mappings
from django.conf import settings
import time
from django.db.models.manager import Manager

def model_to_mapping(model, depth=1):
    """
    Given a model return a mapping
    """
    meta = model._meta
    indexoptions = getattr(model, "indexeroptions", {})
    ignore = indexoptions.get("ignore", [])
    fields_options = indexoptions.get("fields", {})
    extra_fields = indexoptions.get("extra_fields", {})
    mapper = mappings.ObjectField(meta.module_name)
    for field in meta.fields + meta.many_to_many:
        name = field.name
        if name in ignore:
            continue
        mapdata = get_mapping_for_field(field, depth=depth, **fields_options.get(name, {}))
        if mapdata:
            mapper.add_property(mapdata)
    for name, options in extra_fields.items():
        type = options.pop("type", "string")
        if type == "string":
            data = dict(name=name, store=True,
                           index="analyzed",
                           term_vector="with_positions_offsets"
                           )
            data.update(options)

            if  data['index'] == 'not_analyzed':
                del data['term_vector']

            mapper.add_property(mappings.StringField(**data))
            continue

    return mapper

def get_mapping_for_field(field, depth=1, **options):
    """Given a field returns a mapping"""
    ntype = type(field).__name__
    if ntype in ["AutoField"]:
#        return mappings.MultiField(name=field.name,
#                                   fields={field.name:mappings.StringField(name=field.name, store=True),
#                                           "int":mappings.IntegerField(name="int", store=True)}
#                                   )
        return mappings.StringField(name=field.name, store=True)
    elif ntype in ["IntegerField",
                   "PositiveSmallIntegerField",
                   "SmallIntegerField",
                   "PositiveIntegerField",
                   "PositionField",
                   ]:
        return mappings.IntegerField(name=field.name, store=True)
    elif ntype in ["FloatField",
                   "DecimalField",
                   ]:
        return mappings.DoubleField(name=field.name, store=True)
    elif ntype in ["BooleanField",
                   "NullBooleanField",
                   ]:
        return mappings.BooleanField(name=field.name, store=True)
    elif ntype in ["DateField",
                   "DateTimeField",
                   "CreationDateTimeField",
                   "ModificationDateTimeField",
                   "AddedDateTimeField",
                   "ModifiedDateTimeField",
                   "brainaetic.djangoutils.db.fields.CreationDateTimeField",
                   "brainaetic.djangoutils.db.fields.ModificationDateTimeField",
                   ]:
        return mappings.DateField(name=field.name, store=True)
    elif ntype in ["SlugField",
                   "EmailField",
                   "TagField",
                   "URLField",
                   "CharField",
                   "ImageField",
                   "FileField",
                   ]:
        return mappings.MultiField(name=field.name,
                                   fields={field.name:mappings.StringField(name=field.name, index="not_analyzed", store=True),
                                           "tk":mappings.StringField(name="tk", store=True,
                                                                index="analyzed",
                                                                term_vector="with_positions_offsets")}

                                   )
    elif ntype in ["TextField",
                   ]:
        data = dict(name=field.name, store=True,
                       index="analyzed",
                       term_vector="with_positions_offsets"
                       )
        if field.unique:
            data['index'] = 'not_analyzed'

        data.update(options)

        if  data['index'] == 'not_analyzed':
            del data['term_vector']

        return mappings.StringField(**data)
    elif ntype in ["ForeignKey",
                   "TaggableManager",
                   "GenericRelation",
                   ]:
        if depth >= 0:
            mapper = model_to_mapping(field.rel.to, depth - 1)
            if mapper:
                mapper.name = field.name
                return mapper
            return None
        return get_mapping_for_field(field.rel.to._meta.pk, depth - 1)

#                   "IPAddressField",
#                   'PickledObjectField'

    elif ntype in ["ManyToManyField",
                   ]:
        if depth > 0:
            mapper = model_to_mapping(field.rel.to, depth - 1)
            mapper.name = field.name
            return mapper
        if depth == 0:
            mapper = get_mapping_for_field(field.rel.to._meta.pk, depth - 1)
            if mapper:
                mapper.name = field.name
                return mapper
            return None
        if depth < 0:
            return None
    print ntype
    return None

