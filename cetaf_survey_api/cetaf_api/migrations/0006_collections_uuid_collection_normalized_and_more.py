# Generated by Django 5.0.7 on 2024-10-18 12:43

import uuid
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('cetaf_api', '0005_collectionsnormalized_institutionsnormalized_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='collections',
            name='uuid_collection_normalized',
            field=models.UUIDField(default=uuid.uuid4),
        ),
        migrations.AddField(
            model_name='collections',
            name='uuid_institution_normalized',
            field=models.UUIDField(default=uuid.uuid4),
        ),
        migrations.AddField(
            model_name='institutions',
            name='uuid_institution_normalized',
            field=models.UUIDField(default=uuid.uuid4),
        ),
    ]
