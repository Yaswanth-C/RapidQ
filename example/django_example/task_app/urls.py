from django.urls import path

from .views import task_trigger_view

urlpatterns = [path("", task_trigger_view)]
