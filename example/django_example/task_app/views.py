from django.http.response import JsonResponse
from django.shortcuts import render

from .tasks import simple_task


def task_trigger_view(request):
    simple_task.delay()
    return JsonResponse({"task sent": "ok"})
