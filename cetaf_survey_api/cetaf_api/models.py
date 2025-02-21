import uuid as puuid
from django.db import models
from django.db import connection

# Create your models here.
class InstitutionsNormalized(models.Model):
    fpk = models.AutoField(primary_key=True)
    uuid=models.UUIDField( default=puuid.uuid4, editable=False)
    data = models.JSONField()
    creation_date=models.DateTimeField(auto_now_add=True)
    modification_date=models.DateTimeField(auto_now=True)
    
    @staticmethod
    def search_by_ident(protocol, value):
        q=InstitutionsNormalized.objects.extra(where=["uuid IN (SELECT d.uuid  FROM cetaf_api_institutionsnormalized d CROSS JOIN LATERAL jsonb_array_elements(d.data->'list_identifiers') o(obj) WHERE (o.obj->>'type')=%s AND  (o.obj->>'value')=%s)"], params=(protocol, value))
        return q
        
    @staticmethod
    def search_by_uuid(p_uuid):
        returned = None
        q=InstitutionsNormalized.objects.filter(uuid=p_uuid)
        if q is not None:
            print("SEARCH_NORM_INST_IS_NOT_NONE")
            if len(q)>0:
                returned=q.first()
        else:
            print("SEARCH_NORM_INST_IS_NOT_NONE")
        return returned
    """   
    @staticmethod
    def get_identifiers(p_protocol):
        cursor = connection.cursor()
        query='''SELECT data->'list_identifiers',(jsonb_path_query(data->'list_identifiers', '$ ? (@.type=="%s")')->'value')::varchar,
uuid FROM public.cetaf_api_institutionsnormalized;''' % (p_protocol)
        cursor.execute(query)
        rows = cursor.fetchall()
        return rows
    """
    
    def get_all_identifiers_by_protocol(p_protocol):
        cursor = connection.cursor()
        query="SELECT uuid, \
o.obj->>'value' as value \
	FROM public.cetaf_api_institutionsnormalized a \
	cross join lateral jsonb_array_elements(a.data->'list_identifiers') o(obj) \
	where (obj->>'type'='%s');" % (p_protocol)
        cursor.execute(query)
        rows = cursor.fetchall()
        rows=[dict(zip([col[0] for col in cursor.description ], row)) for row in  rows]      
        return rows
        
    class Meta:
        ordering = ["uuid"]
    
class CollectionsNormalized(models.Model):
    fpk = models.AutoField(primary_key=True)
    fk_institution_normalized=models.ForeignKey(InstitutionsNormalized, on_delete=models.PROTECT)
    fk_parent_collection_normalized=models.ForeignKey('self', on_delete=models.PROTECT, default=None, blank=True, null=True)
    uuid=models.UUIDField( default=puuid.uuid4, editable=False)
    uuid_institution_normalized=models.UUIDField( default=puuid.uuid4)
    local_identifier = models.CharField()
    data = models.JSONField()
    creation_date=models.DateTimeField(auto_now_add=True)
    modification_date=models.DateTimeField(auto_now=True)

 
    @staticmethod
    def search_by_ident(uuid_inst, protocol, value):
        q=CollectionsNormalized.objects.extra(where=["uuid_institution_normalized=%s AND uuid IN (SELECT d.uuid  FROM cetaf_api_collectionsnormalized d CROSS JOIN LATERAL jsonb_array_elements(d.data->'list_identifiers') o(obj) WHERE (o.obj->>'type')=%s AND  (o.obj->>'value')=%s)"], params=(uuid_inst, protocol, value))
        return q
        
    @staticmethod
    def search_by_uuid(p_uuid):
        returned = None
        q=CollectionsNormalized.objects.filter(uuid=p_uuid)
        if q is not None:
            if len(q)>0:
                returned=q.first()
        return returned
        
    class Meta:
        ordering = ["uuid"]
        
class Institutions(models.Model):
    fpk = models.AutoField(primary_key=True)
    fk_institution_normalized=models.ForeignKey(InstitutionsNormalized, on_delete=models.PROTECT)    
    uuid=models.UUIDField( default=puuid.uuid4, editable=False)
    uuid_institution_normalized=models.UUIDField( default=puuid.uuid4)
    identifier = models.CharField()
    data = models.JSONField()
    harvesting_date=models.DateTimeField(auto_now_add=True)
    modification_date=models.DateTimeField(auto_now=False)
    version=models.IntegerField()
    current=models.BooleanField()

    
    def get_identifiers(self):
        returned={}        
        if "list_identifiers" in self.data:
            print(self.data["list_identifiers"])
            for ident in self.data["list_identifiers"]:
                returned[ident["type"].lower()]=ident["value"]                
        return returned
    
    
        
    class Meta:
        ordering = ["identifier",'modification_date']
        

    
class Collections(models.Model):
    fpk = models.AutoField(primary_key=True)
    #fk_institution=models.ForeignKey(Institutions, on_delete=models.PROTECT)
    fk_collection_normalized=models.ForeignKey(CollectionsNormalized, on_delete=models.PROTECT)
    fk_institution_normalized=models.ForeignKey(InstitutionsNormalized, on_delete=models.PROTECT)
    #fk_parent_collection_normalized=models.ForeignKey(CollectionsNormalized, on_delete=models.PROTECT, default=None, blank=True, null=True)
    fk_parent_collection_current=models.ForeignKey('self', on_delete=models.PROTECT, default=None, blank=True, null=True)
    uuid=models.UUIDField(default=puuid.uuid4, editable=False)
    uuid_institution_normalized=models.UUIDField( default=puuid.uuid4)
    uuid_collection_normalized=models.UUIDField( default=puuid.uuid4)
    uuid_parent_collection_normalized=models.UUIDField( default=puuid.uuid4, blank=True, null=True)
    uuid_parent_collection_current=models.UUIDField( default=puuid.uuid4, blank=True, null=True)
    identifier = models.CharField()
    local_identifier = models.CharField()
    source_uri = models.JSONField(default=None, blank=True, null=True)
    data = models.JSONField()
    creation_date=models.DateTimeField(auto_now_add=True)
    modification_date=models.DateTimeField(auto_now=True)
    version=models.IntegerField()
    current=models.BooleanField()
    
    class Meta:
        ordering = ["identifier",'modification_date']
        
    
    
class GoogleSheetIndexResponses(models.Model):
    fpk = models.AutoField(primary_key=True)
    google_id=models.JSONField(default=None, blank=True, null=True)
    title=models.CharField()
    list_sheets=models.CharField(default=None, blank=True, null=True)
    path=models.CharField(default=None, blank=True, null=True)
    data = models.JSONField(default=None, blank=True, null=True)
    modified_date=models.DateTimeField(default=None, blank=True, null=True)
    harvesting_date=models.DateTimeField()
    version=models.IntegerField()
    current=models.BooleanField()    
    
class GoogleSheetCollectionReply(models.Model):
    fpk = models.AutoField(primary_key=True)
    google_id=models.JSONField(default=None, blank=True, null=True)
    fk_index_response=models.ForeignKey(GoogleSheetIndexResponses, on_delete=models.PROTECT, default=None, blank=True, null=True)
    cetaf_institution_id=models.CharField(default=None, blank=True, null=True)
    institution_uuid=models.UUIDField(default=None, blank=True, null=True)
    institution_cetaf_acronym=models.CharField(default=None, blank=True, null=True)
    collection_id=models.CharField(default=None, blank=True, null=True)
    collection_cetaf_acronym=models.CharField(default=None, blank=True, null=True)
    collection_uuid=models.UUIDField(default=puuid.uuid4, blank=True, null=True)
    sub_collection_id=models.CharField(default=None, blank=True, null=True)    
    sub_collection_uuid=models.UUIDField(default=puuid.uuid4, blank=True, null=True)
    sub_collection_cetaf_acronym=models.CharField(default=None, blank=True, null=True)
    mime_type=models.CharField()
    title=models.CharField()
    list_sheets=models.CharField(default=None, blank=True, null=True)
    path=models.CharField(default=None, blank=True, null=True)
    data = models.JSONField(default=None, blank=True, null=True)
    metadata_from_index = models.JSONField(default=None, blank=True, null=True)
    modified_date=models.DateTimeField(default=None, blank=True, null=True)
    harvesting_date=models.DateTimeField()
    version=models.IntegerField()
    current=models.BooleanField()
    