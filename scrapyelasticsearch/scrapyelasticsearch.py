# Copyright 2014 Michael Malocha <michael@knockrentals.com>
#
# Expanded from the work by Julien Duponchelle <julien@duponchelle.info>.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Elastic Search Pipeline for scrappy expanded with support for multiple items"""

from datetime import datetime
from elasticsearch import Elasticsearch, helpers
from six import string_types

import logging
import hashlib
import types


class InvalidSettingsException(Exception):
    pass


class ElasticSearchPipeline(object):

    def __init__(self, settings):
        self.settings = settings

        self.buffer_size = settings.get('ELASTICSEARCH_BUFFER_LENGTH', 50)
        self.index = settings.get('ELASTICSEARCH_INDEX')
        self.doc_type = settings.get('ELASTICSEARCH_TYPE')
        self.es = self._get_es_instance()

        self.items_buffer = []
        self.es = self._get_es_instance()

    def _get_es_instance(self):
        settings = self.settings

        auth_type = settings.get('ELASTICSEARCH_AUTH')
        hosts = settings.getlist('ELASTICSEARCH_SERVERS')

        if auth_type == 'NTLM':
            from .transportNTLM import TransportNTLM
            return Elasticsearch(hosts=hosts,
                                 transport_class=TransportNTLM,
                                 ntlm_user=settings.get('ELASTICSEARCH_USERNAME'),
                                 ntlm_pass=settings.get('ELASTICSEARCH_PASSWORD'),
                                 timeout=settings.get('ELASTICSEARCH_TIMEOUT', 60))

        elif auth_type == 'BASIC_AUTH':
            use_ssl = settings.getbool('ELASTICSEARCH_USE_SSL')
            auth_tuple = (settings.get('ELASTICSEARCH_USERNAME'),
                          settings.get('ELASTICSEARCH_PASSWORD'))
            if use_ssl:
                return Elasticsearch(hosts=hosts,
                                     http_auth=auth_tuple,
                                     port=443, use_ssl=True)
            else:
                return Elasticsearch(hosts=hosts,
                                     http_auth=auth_tuple)
        else:
            return Elasticsearch(hosts=hosts)

    @classmethod
    def validate_settings(cls, settings):
        def validate_setting(setting_key):
            if settings[setting_key] is None:
                raise InvalidSettingsException('%s is not defined in settings.py' % setting_key)

        required_settings = {'ELASTICSEARCH_INDEX', 'ELASTICSEARCH_TYPE'}

        for required_setting in required_settings:
            validate_setting(required_setting)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    @staticmethod
    def get_unique_key(unique_key):
        if isinstance(unique_key, list):
            unique_key = unique_key[0].encode('utf-8')
        elif isinstance(unique_key, string_types):
            unique_key = unique_key.encode('utf-8')
        else:
            raise Exception('unique key must be str or unicode')

        return unique_key

    def index_item(self, item):
        index_action = {
            '_index': self.index,
            '_type': self.doc_type
        }

        unique_key = self.settings['ELASTICSEARCH_UNIQ_KEY']
        if unique_key is not None:
            item_unique_key = item[unique_key]
            unique_key = self.get_unique_key(item_unique_key)

            item_id = hashlib.sha1(unique_key).hexdigest()
            index_action['_id'] = item_id
            logging.debug('Generated unique key %s' % item_id)

        self.items_buffer.append({'index': index_action})
        self.items_buffer.append(dict(item))

        if len(self.items_buffer) >= self.buffer_size:
            self.send_items()
            self.items_buffer = []

    def send_items(self):
        self.es.bulk(body=self.items_buffer, index=self.index, doc_type=self.doc_type)

    def process_item(self, item, spider):
        if isinstance(item, types.GeneratorType) or isinstance(item, list):
            for each in item:
                self.process_item(each, spider)
        else:
            self.index_item(item)
            logging.debug('Item sent to Elastic Search %s' % self.settings['ELASTICSEARCH_INDEX'])
            return item

    def close_spider(self, spider):
        if len(self.items_buffer):
            self.send_items()