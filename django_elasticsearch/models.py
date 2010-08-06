from django.db.models import signals
from .fields import add_elasticsearch_manager

signals.class_prepared.connect(add_elasticsearch_manager)