#!/usr/bin/env python3
# pylint: disable=too-many-arguments,invalid-name,too-many-nested-blocks

"""
Singer.io target for Retention Science (ReSci) API.
Web: retentionscience.com
Docs: developer.retentionscience.com
"""

import argparse
import http.client
import io
import json
import os
import sys
import time
import urllib
import random
import string

from threading import Thread
from contextlib import contextmanager
from collections import namedtuple, MutableMapping
from datetime import datetime
from decimal import Decimal
import psutil

import requests
from requests.exceptions import RequestException, HTTPError
import pkg_resources
import singer
import backoff

LOGGER = singer.get_logger().getChild('target_resci')

# We use this to store schema and key properties from SCHEMA messages
StreamMeta = namedtuple('StreamMeta', ['schema', 'key_properties', 'bookmark_properties'])

DEFAULT_RESCI_URL = 'https://data.retentionscience.com/v3/import_jobs'
DEFAULT_MAX_BATCH_BYTES = 4000000
DEFAULT_MAX_BATCH_RECORDS = 1000000


class TargetResciException(Exception):
    """A known exception for which we don't need to print a stack trace"""
    pass


class MemoryReporter(Thread):
    """Logs memory usage every 30 seconds"""

    def __init__(self):
        self.process = psutil.Process()
        super().__init__(name='memory_reporter', daemon=True)

    def run(self):
        while True:
            LOGGER.debug('Virtual memory usage: %.2f%% of total: %s',
                         self.process.memory_percent(),
                         self.process.memory_info())
            time.sleep(30.0)


class Timings(object):
    """Gathers timing information for the three main steps of the Tap."""
    def __init__(self):
        self.last_time = time.time()
        self.timings = {
            'serializing': 0.0,
            'posting': 0.0,
            None: 0.0
        }

    @contextmanager
    def mode(self, mode):
        """We wrap the big steps of the Tap in this context manager to accumulate
        timing info."""

        start = time.time()
        yield
        end = time.time()
        self.timings[None] += start - self.last_time
        self.timings[mode] += end - start
        self.last_time = end

    def log_timings(self):
        """We call this with every flush to print out the accumulated timings"""
        LOGGER.debug('Timings: unspecified: %.3f; serializing: %.3f; posting: %.3f;',
                     self.timings[None],
                     self.timings['serializing'],
                     self.timings['posting'])


TIMINGS = Timings()


def float_to_decimal(value):
    """Walk the given data structure and turn all instances of float into
    double."""
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [float_to_decimal(child) for child in value]
    if isinstance(value, dict):
        return {k: float_to_decimal(v) for k, v in value.items()}
    return value


def _log_backoff(details):
    (_, exc, _) = sys.exc_info()
    LOGGER.info(
        'Error sending data to ReSci. Sleeping %d seconds before trying again: %s',
        details['wait'], exc)


class ResciHandler(object):  # pylint: disable=too-few-public-methods
    """Sends messages to ReSci."""

    def __init__(self, api_key, import_type, resci_url):
        self.api_key = api_key
        self.import_type = import_type
        self.resci_url = resci_url
        self.file_id = self.make_file_id()
        self.session = requests.Session()

    @staticmethod
    def make_file_id():
        """Return a unique time-based id for filename"""
        now_part = datetime.now().strftime('%Y%m%d-%H%M%S')
        random_part = ''.join([random.choice(string.ascii_lowercase) for _ in range(2)])
        return "{}-{}".format(now_part, random_part)

    def headers(self):
        """Return the headers based on the api_key"""
        return {
            'Authorization': 'ApiKey {}'.format(self.api_key)
        }

    @backoff.on_exception(backoff.expo,
                          RequestException,
                          giveup=singer.utils.exception_is_4xx,
                          max_tries=8,
                          on_backoff=_log_backoff)
    def send(self, file_type, file_name):
        """Send the given data to ReSci, retrying on exceptions"""
        url = self.resci_url
        headers = self.headers()
        ssl_verify = True
        if os.environ.get("TARGET_RESCI_SSL_VERIFY") == 'false':
            ssl_verify = False

        files = {file_type: open(file_name, 'rb')}
        params = {'import_type': self.import_type}
        response = self.session.post(url,
                                     data=params,
                                     headers=headers,
                                     files=files,
                                     verify=ssl_verify)

        response.raise_for_status()
        return response

    def flatten(self, d, parent_key='', sep='__'):
        """Flattens dictionary"""
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, MutableMapping):
                items.extend(self.flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, str(v) if type(v) is list else v))
        return dict(items)

    def create_file(self, messages, file_type, batch_count):
        """Creates a file and writes JSON"""
        filename = '{}-{}-{:03}.json'.format(file_type, self.file_id, batch_count)
        LOGGER.debug('Filename: %s', filename)
        with open(filename, "w") as outfile:
            for msg in messages:
                flattened_record = self.flatten(msg.record)
                json.dump(flattened_record, outfile)
                outfile.write("\n")
        return filename

    def handle_batch(self, messages, batch_count, dry_run):
        """Handle messages by sending them to ReSci as import file.

        """
        file_type = messages[0].stream
        LOGGER.info("Sending batch with %d messages for table %s to %s",
                    len(messages), file_type, self.resci_url)

        with TIMINGS.mode('serializing'):
            file_name = self.create_file(messages, file_type, batch_count)

        with TIMINGS.mode('posting'):
            file_size = os.stat(file_name).st_size
            LOGGER.debug('Posting %s file %s of size %d', file_type, file_name, file_size)
            try:
                if not dry_run:
                    response = self.send(file_type, file_name)
                    LOGGER.debug('Response is {}: {}'.format(response, response.content))
                os.remove(file_name)

            # An HTTPError means we got an HTTP response but it was a
            # bad status code. Try to parse the "message" from the
            # json body of the response, since ReSci should include
            # the human-oriented message in that field. If there are
            # any errors parsing the message, just include the
            # stringified response.
            except HTTPError as exc:
                try:
                    response_body = exc.response.json()
                    if isinstance(response_body, dict) and 'message' in response_body:
                        msg = response_body['message']
                    else:
                        msg = '{}: {}'.format(exc.response, exc.response.content)
                except:  # pylint: disable=bare-except
                    LOGGER.exception('Exception while processing error response')
                    msg = '{}: {}'.format(exc.response, exc.response.content)
                raise TargetResciException('Error persisting data for ' +
                                           '"' + file_type + '": ' +
                                           msg)
            # A RequestException other than HTTPError means we
            # couldn't even connect to ReSci. The exception is likely
            # to be very long and gross. Log the full details but just
            # include the summary in the critical error message.
            except RequestException as exc:
                LOGGER.exception(exc)
                raise TargetResciException('Error connecting to ReSci')


class TargetResci(object):
    """Encapsulates most of the logic of target-resci.
    Useful for unit testing.

    """

    # pylint: disable=too-many-instance-attributes
    def __init__(self,  # pylint: disable=too-many-arguments
                 handlers,
                 state_writer,
                 max_batch_bytes,
                 max_batch_records,
                 batch_delay_seconds,
                 dry_run):
        self.messages = []
        self.buffer_size_bytes = 0
        self.state = None
        self.batch_count = 0
        self.dry_run = dry_run

        # Instance of ResciHandler
        self.handlers = handlers

        # Writer that we write state records to
        self.state_writer = state_writer

        # Batch size limits. Stored as properties here so we can easily
        # change for testing.
        self.max_batch_bytes = max_batch_bytes
        self.max_batch_records = max_batch_records

        # Minimum frequency to send a batch, used with self.time_last_batch_sent
        self.batch_delay_seconds = batch_delay_seconds

        # Time that the last batch was sent
        self.time_last_batch_sent = time.time()

    def flush(self):
        """Send all the buffered messages to ReSci."""

        if self.messages:
            self.batch_count += 1
            for handler in self.handlers:
                handler.handle_batch(self.messages,
                                     self.batch_count,
                                     self.dry_run)
            self.time_last_batch_sent = time.time()
            self.messages = []
            self.buffer_size_bytes = 0

        if self.state:
            line = json.dumps(self.state)
            self.state_writer.write("{}\n".format(line))
            self.state_writer.flush()
            self.state = None
            TIMINGS.log_timings()

    def handle_line(self, line):
        """Takes a raw line from stdin and handles it, updating state and possibly
        flushing the batch to the Gate and the state to the output
        stream.

        """

        message = singer.parse_message(line)

        if isinstance(message, (singer.RecordMessage, singer.ActivateVersionMessage)):
            if self.messages and (
                    message.stream != self.messages[0].stream or
                    message.version != self.messages[0].version):
                self.flush()
            self.messages.append(message)
            self.buffer_size_bytes += len(line)

            num_bytes = self.buffer_size_bytes
            num_messages = len(self.messages)
            num_seconds = time.time() - self.time_last_batch_sent

            enough_bytes = num_bytes >= self.max_batch_bytes
            enough_messages = num_messages >= self.max_batch_records
            enough_time = num_seconds >= self.batch_delay_seconds
            if enough_bytes or enough_messages or enough_time:
                LOGGER.debug('Flushing %d bytes, %d messages, after %.2f seconds',
                             num_bytes, num_messages, num_seconds)
                self.flush()

        elif isinstance(message, singer.StateMessage):
            self.state = message.value

    def consume(self, reader):
        """Consume all the lines from the queue, flushing when done."""
        for line in reader:
            self.handle_line(line)
        self.flush()


def main_impl():
    """We wrap this function in main() to add exception handling"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-c', '--config',
        help='Config file',
        type=argparse.FileType('r'))
    parser.add_argument(
        '-n', '--dry-run',
        help='Dry run - Do not push data to ReSci',
        action='store_true')
    parser.add_argument(
        '-v', '--verbose',
        help='Produce debug-level logging',
        action='store_true')
    parser.add_argument(
        '-q', '--quiet',
        help='Suppress info-level logging',
        action='store_true')
    parser.add_argument('--max-batch-records', type=int, default=DEFAULT_MAX_BATCH_RECORDS)
    parser.add_argument('--max-batch-bytes', type=int, default=DEFAULT_MAX_BATCH_BYTES)
    parser.add_argument('--batch-delay-seconds', type=float, default=300.0)
    args = parser.parse_args()

    if args.verbose:
        LOGGER.setLevel('DEBUG')
    elif args.quiet:
        LOGGER.setLevel('WARNING')

    handlers = []

    if args.dry_run:
        dry_run = True
    else:
        dry_run = False

    if not args.config:
        parser.error("config file required if not in dry run mode")
    else:
        config = json.load(args.config)
        api_key = config.get('api_key')
        if not api_key:
            raise Exception('Configuration is missing required "api_key" field')

        import_type = config.get('import_type')
        if not import_type:
            raise Exception('Configuration is missing required "import_type" field')

        api_url = config.get('api_url')
        if not api_url:
            api_url = DEFAULT_RESCI_URL

        if not config.get('disable_collection'):
            LOGGER.info('Sending version information to stitchdata.com. ' +
                        'To disable sending anonymous usage data, set ' +
                        'the config parameter "disable_collection" to true')
            Thread(target=collect).start()

        handlers.append(ResciHandler(api_key, import_type, api_url))

    reader = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    TargetResci(handlers,
                sys.stdout,
                args.max_batch_bytes,
                args.max_batch_records,
                args.batch_delay_seconds,
                dry_run).consume(reader)
    LOGGER.info("Exiting normally")


def collect():
    """Send usage info to Singer collector."""

    try:
        version = pkg_resources.get_distribution('target-resci').version
        conn = http.client.HTTPSConnection('collector.stitchdata.com', timeout=10)
        conn.connect()
        params = {
            'e': 'se',
            'aid': 'singer',
            'se_ca': 'target-resci',
            'se_ac': 'open',
            'se_la': version,
        }
        request_url = '/i?' + urllib.parse.urlencode(params)
        LOGGER.debug('Collection tracking info to Singer: ' + request_url)
        conn.request('GET', request_url)
        conn.getresponse()
        conn.close()
    except Exception as e:
        LOGGER.debug('Collection request failed' + str(e))


def main():
    """Main entry point"""
    try:
        MemoryReporter().start()
        main_impl()

    # If we catch an exception at the top level we want to log a CRITICAL
    # line to indicate the reason why we're terminating. Sometimes the
    # extended stack traces can be confusing and this provides a clear way
    # to call out the root cause. If it's a known TargetResciException we
    # can suppress the stack trace, otherwise we should include the stack
    # trace for debugging purposes, so re-raise the exception.
    except TargetResciException as exc:
        for line in str(exc).splitlines():
            LOGGER.critical(line)
        sys.exit(1)
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc


if __name__ == '__main__':
    main()
