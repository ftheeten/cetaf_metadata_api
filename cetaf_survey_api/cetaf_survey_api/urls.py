"""
URL configuration for cetaf_survey_api project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from rest_framework import routers, serializers, viewsets
from cetaf_api.views import WSIInstitutionsView, WSICollectionsView, WSIGoogleSheetView
from django.conf.urls import include

#https://backendengineer.io/django-rest-framework-file-upload-api/

urlpatterns = [
    path('cetaf_survey_api/institutions/', WSIInstitutionsView.as_view()),
    path('cetaf_survey_api/collections/', WSICollectionsView.as_view()),
    path('cetaf_survey_api/excel_in_cloud/', WSIGoogleSheetView.as_view()),
]

"""
router = routers.DefaultRouter()
router.register('institutions', WSIInstitutionsView, basename="cetaf_survey_api")

urlpatterns = [
    path('admin/', admin.site.urls),
    path('cetaf_survey_api/', include(router.urls)),
]
"""
