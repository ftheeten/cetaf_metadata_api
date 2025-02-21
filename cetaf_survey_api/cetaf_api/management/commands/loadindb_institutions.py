from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from ...parser.gs_cetaf_parser import GSCetafParser

#https://docs.djangoproject.com/fr/5.0/howto/custom-management-commands/

#example:
#python manage.py loadindb --extra_apis  grscicoll_institutions grscicoll_collections_from_institutions
#python manage.py loadindb --extra_apis  collection_overview

class Command(BaseCommand):
    
     
    
    def add_arguments(self, parser):
        parser.add_argument("--extra_apis", nargs="+",  default=[])
        parser.add_argument("--force", nargs="*",  default=[])
        
    def handle(self, *args, **options):
        print("reload")
        print(options['extra_apis'])
        print(options)
        print(args)
        sheet_institutions=settings.CETAF_DATA_ADDRESS["institutions"]
        print(sheet_institutions)
        gs_parser=GSCetafParser()
        if  "institution_overview" in options['extra_apis'] or "grscicoll_institutions" in options['extra_apis'] : #or "grscicoll_institutions" in options['extra_apis']:
            force=False
            if "force" in options:
                if "true" in options['force']:
                    force=True
                    print("_FORCE_")
            gs_parser.load_institution_sheet(options['extra_apis'], force)
        elif "collection_overview" in options['extra_apis']:
            print("collection overview")
            gs_parser.load_collection_overview_sheet(options['extra_apis'])

            
            
            