# Copyright 2018 Immuta, Inc. Licensed under the Immuta Software License
# Version 0.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
#
#    http://www.immuta.com/licenses/Immuta_Software_License_0.1.txt
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import logging
import sys
from six import string_types

try:
    from logging.config import dictConfig
except ImportError:
    from pip.compat.dictconfig import dictConfig  # type: ignore

logging_config = {
    "version": 1,
    "formatters": {"default": {"format": "%(levelname)s:%(name)s:%(message)s"}},
    "filters": {"limit": {"()": "immuta.log.ReverseLevelFilter"}},
    "handlers": {
        "standard": {
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
            "formatter": "default",
            "filters": ["limit"],
        },
        "error": {
            "class": "logging.StreamHandler",
            "formatter": "default",
            "level": "ERROR",
        },
    },
    "root": {"level": "INFO", "handlers": ["standard", "error"]},
}


class ReverseLevelFilter(logging.Filter):
    """
    Reverse log level Filter
    Causes only log messages less than or equal to level to be processed
    """

    def __init__(self, level=logging.INFO):
        self.level = level

    def filter(self, record):
        return 1 if record.levelno <= self.level else 0


class LoggingMixin(object):
    """
    Logging Mixin class
    Adds a log property configured with the class's logger
    """

    @property
    def logger_name(self):
        if not hasattr(self, "__logger_name"):
            self.__logger_name = "{0}.{1}".format(
                self.__module__, self.__class__.__name__
            )
        return self.__logger_name

    @property
    def log(self):
        return logging.getLogger(self.logger_name)


def init(level=None, debug=[], log_format=None):
    """
    Initialize logging with the default logging configuration

    debug - package(s) to enable debug logging for
    log_format - format for logs
    """
    # if log_format:
    #     logging_config['formatters']['default']['format'] = log_format
    # else:
    #     logging_config['formatters']['default']['format'] = settings['log.format']

    dictConfig(logging_config)

    root_level = "DEBUG"
    logging.getLogger().setLevel(root_level)

    if isinstance(debug, string_types):
        debug = [debug]
    for package in debug:
        logging.getLogger(package).setLevel(logging.DEBUG)
