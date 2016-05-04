__author__ = 'max'

import importlib


class Settings(object):
    """
    This class provides a buffer between the settings module and the objects
    using it, allowing an injection point for unit tests.
    """
    _settings_mod_cache = None

    @property
    def _settings_mod(self):
        if not self._settings_mod_cache:
            self._settings_mod_cache = importlib.import_module('settings')

        return self._settings_mod_cache

    def __getattr__(self, name):
        return getattr(self._settings_mod, name)

settings = Settings()
