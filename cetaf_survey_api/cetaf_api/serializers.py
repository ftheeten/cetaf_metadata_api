from rest_framework import serializers
from .models import Institutions, Collections

class InstitutionSerializer(serializers.ModelSerializer):
    class Meta:
        model = Institutions
        fields = ['uuid', 'uuid_institution_normalized', 'current', 'version','identifier', 'data','modification_date', 'harvesting_date']
        
class CollectionSerializer(serializers.ModelSerializer):
    class Meta:
        model = Collections
        fields = ['uuid', 'uuid_institution_normalized','uuid_collection_normalized', 'current', 'version','identifier', 'local_identifier', 'data','modification_date']