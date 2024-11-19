import uuid as puuid
from django.db import models

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
        
    class Meta:
        ordering = ["uuid"]
    
class CollectionsNormalized(models.Model):
    fpk = models.AutoField(primary_key=True)
    fk_institution_normalized=models.ForeignKey(InstitutionsNormalized, on_delete=models.PROTECT)
    uuid=models.UUIDField( default=puuid.uuid4, editable=False)
    uuid_institution_normalized=models.UUIDField( default=puuid.uuid4)
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
        
    #query JSONB
    """
    select  o.obj , o.obj->'type', *  FROM public.cetaf_api_institutions d
cross join lateral jsonb_array_elements(d.data->'list_identifiers') o(obj)
where (o.obj->>'type')='ror' and  (o.obj->>'value')='02v6zg374'
    """
    
class Collections(models.Model):
    fpk = models.AutoField(primary_key=True)
    #fk_institution=models.ForeignKey(Institutions, on_delete=models.PROTECT)
    fk_collection_normalized=models.ForeignKey(CollectionsNormalized, on_delete=models.PROTECT)
    fk_institution_normalized=models.ForeignKey(InstitutionsNormalized, on_delete=models.PROTECT)
    uuid=models.UUIDField(default=puuid.uuid4, editable=False)
    uuid_institution_normalized=models.UUIDField( default=puuid.uuid4)
    uuid_collection_normalized=models.UUIDField( default=puuid.uuid4)
    identifier = models.CharField()
    source_uri = models.CharField()
    data = models.JSONField()
    creation_date=models.DateTimeField(auto_now_add=True)
    modification_date=models.DateTimeField(auto_now=True)
    version=models.IntegerField()
    current=models.BooleanField()
    
    class Meta:
        ordering = ["identifier",'modification_date']