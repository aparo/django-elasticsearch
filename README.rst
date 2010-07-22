===========================
Django ElasticSearch Engine
===========================
:Info: It's a database backend that adds elasticsearch support to django
:Author: Alberto [aparo] Paro (http://github.com/aparo)

Requirements
------------

- Django non rel http://github.com/aparo/django-nonrel
- Djangotoolbox http://github.com/aparo/djangotoolbox
- pyes http://github.com/aparo/pyes


About Django
============

Django is a high-level Python Web framework that encourages rapid development and clean, pragmatic design.

About ElasticSearch
===================

TODO

Infographics
============
::
    - Django Nonrel branch
    - Manager
    - Compiler (ElasticSearch Engine one)
    - ElasticSearch

django-elasticsearch uses the new django1.2 multi-database support and sets to the model the database using the "django_elasticsearch".

Examples
========

::

    class Person(models.Model):
        name = models.CharField(max_length=20)
        surname = models.CharField(max_length=20)
        age = models.IntegerField(null=True, blank=True)
                
        def __unicode__(self):
            return u"Person: %s %s" % (self.name, self.surname)

    >> p, created = Person.objects.get_or_create(name="John", defaults={'surname' : 'Doe'})
    >> print created
    True
    >> p.age = 22
    >> p.save()

    === Querying ===
    >> p = Person.objects.get(name__istartswith="JOH", age=22)
    >> p.pk
    u'4bd212d9ccdec2510f000000'
