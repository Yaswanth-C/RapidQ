import os
from importlib import import_module

from rapidq.constants import DEFAULT_AUTO_DISCOVER_MODULES
from rapidq.registry import framework_loader

LOAD_ERROR = "Django is not installed!"


@framework_loader
def load_django(worker):
    django_settings_module = os.environ.get("DJANGO_SETTINGS_MODULE")
    if not django_settings_module:
        return None

    try:
        import django
    except ImportError:
        raise RuntimeError(LOAD_ERROR)
    else:
        DjangoSetup().setup()
    return None


class DjangoSetup:
    def setup_django(self) -> None:
        import django

        django.setup()

    def autodiscover_tasks(self, django_settings) -> None:
        auto_discover_modules = getattr(
            django_settings,
            "RAPIDQ_TASK_DISCOVER_MODULES",
            DEFAULT_AUTO_DISCOVER_MODULES,
        )
        installed_apps = getattr(django_settings, "INSTALLED_APPS", [])

        for app in installed_apps:
            for _module in auto_discover_modules:
                try:
                    app_module_name = app.partition(".")[0]
                    task_module_name = f"{app_module_name}.{_module}"
                    import_module(task_module_name)
                except ImportError:
                    pass
        return None

    def setup(self) -> None:
        django_settings = getattr(import_module("django.conf"), "settings")
        if django_settings.configured:
            return None
        self.setup_django()
        self.autodiscover_tasks(django_settings)
        return None
