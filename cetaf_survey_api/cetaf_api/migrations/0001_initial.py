# Generated by Django 5.0.7 on 2024-07-27 21:48

import django.db.models.deletion
import uuid
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Institutions',
            fields=[
                ('fpk', models.AutoField(primary_key=True, serialize=False)),
                ('uuid', models.UUIDField(default=uuid.uuid4, editable=False)),
                ('identifier', models.CharField()),
                ('data', models.JSONField()),
                ('creation_date', models.DateTimeField(auto_now_add=True)),
                ('modification_date', models.DateTimeField(auto_now=True)),
                ('current', models.BooleanField()),
            ],
            options={
                'ordering': ['identifier', 'modification_date'],
            },
        ),
        migrations.CreateModel(
            name='Collections',
            fields=[
                ('fpk', models.AutoField(primary_key=True, serialize=False)),
                ('uuid', models.UUIDField(default=uuid.uuid4, editable=False)),
                ('identifier', models.CharField()),
                ('data', models.JSONField()),
                ('creation_date', models.DateTimeField(auto_now_add=True)),
                ('modification_date', models.DateTimeField(auto_now=True)),
                ('current', models.BooleanField()),
                ('fk_institution', models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='cetaf_api.institutions')),
            ],
            options={
                'ordering': ['identifier', 'modification_date'],
            },
        ),
    ]
