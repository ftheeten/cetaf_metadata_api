from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from ...parser.gs_cetaf_collections_parser import GSCetafCollectionsParser

class Command(BaseCommand):

    def add_arguments(self, parser):
        pass

    def handle(self, *args, **options):
        print("coll load")
        parser=GSCetafCollectionsParser(settings.CETAF_DATA_ADDRESS['collection_root_folder'])
        parser.explore_drive()
        parser.process_collections_details_from_reply_index()