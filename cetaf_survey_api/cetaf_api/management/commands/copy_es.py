from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from ...parser.es_loader import ESLoader

class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("--target_index", nargs="+",  default=[])
        
    def handle(self, *args, **options):
        es_loader=ESLoader()
        if "institutions" in options['target_index']:
            print("institutions")
            es_loader.delete_all_institutions()
            es_loader.load_current_institutions()
        elif "collections" in options['target_index']:
            print("collections")
            es_loader.delete_all_collections()
            es_loader.load_current_collections()