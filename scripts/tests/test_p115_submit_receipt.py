"""Offline receipt and task-completion regressions; no real 115 client."""
from __future__ import annotations

import base64
import json
import unittest

from pipeline.adapters import RemoteInventoryIncomplete, RemoteRateLimited
from pipeline.real_adapters import P115CloudAdapter


TARGET = '9' * 40
SECRET = 'private-token-do-not-emit'


class ReceiptClient:
    def __init__(self, response=None, failure=None):
        self.response = response
        self.failure = failure
        self.calls = []

    def clouddownload_task_add_bt(self, *args, **kwargs):
        raise AssertionError('physical add_bt must never be called')

    def clouddownload_task_add_url(self, payload, *, type):
        self.calls.append((dict(payload), type))
        if self.failure is not None:
            raise self.failure
        return self.response


def adapter(client, **kwargs):
    return P115CloudAdapter(None, '1', client=client, **kwargs)


class SubmitReceiptTests(unittest.TestCase):
    def submit(self, response):
        client = ReceiptClient(response)
        value = adapter(client).submit(TARGET)
        self.assertEqual(client.calls, [({'url': 'magnet:?xt=urn:btih:' + TARGET, 'wp_path_id': 1}, 'ssp')])
        return value

    def assert_rejected(self, response, code=None):
        with self.assertRaises(RemoteInventoryIncomplete) as raised:
            self.submit(response)
        self.assertNotIsInstance(raised.exception, RemoteRateLimited)
        self.assertNotIn(SECRET, str(raised.exception))
        if code:
            self.assertEqual(str(raised.exception), code)

    def test_standard_magnet_and_raw_hash_use_one_canonical_url_call(self):
        for value in (TARGET, TARGET.upper(), 'magnet:?xt=urn:btih:' + TARGET,
                      'magnet:?xt=urn:btih:' + TARGET.upper() + '&dn=ignored&tr=https://tracker.invalid'):
            with self.subTest(value=value):
                client = ReceiptClient({'state': True, 'data': {'info_hash': TARGET}})
                self.assertEqual(adapter(client).submit(value), TARGET)
                self.assertEqual(client.calls, [({'url': 'magnet:?xt=urn:btih:' + TARGET,
                                                 'wp_path_id': 1}, 'ssp')])

    def test_arbitrary_urls_and_malformed_magnets_never_call_sdk(self):
        for value in ('https://example.invalid/?xt=urn:btih:' + TARGET,
                      'ed2k://file|' + TARGET, 'file:///tmp/' + TARGET,
                      'magnet:?xt=urn:btih:' + TARGET + 'extra',
                      'magnet:?xt=urn:btih:' + TARGET + '&xt=urn:btih:' + TARGET,
                      'magnet://host/?xt=urn:btih:' + TARGET,
                      'magnet://[malformed/?xt=urn:btih:' + TARGET):
            with self.subTest(value=value):
                client = ReceiptClient({'state': True, 'info_hash': TARGET})
                with self.assertRaises(ValueError) as raised:
                    adapter(client).submit(value)
                self.assertEqual(str(raised.exception), '115_submit_input_invalid')
                self.assertEqual(client.calls, [])

    def test_explicit_success_with_matching_hash(self):
        self.assertEqual(self.submit({'state': True, 'data': {'info_hash': TARGET}}), TARGET)
        self.assertEqual(self.submit({'success': True, 'errno': 0, 'infohash': TARGET}), TARGET)

    def test_encoded_success_and_normalized_hash(self):
        self.assertEqual(self.submit(json.dumps({'state': True, 'data': {'info_hash': TARGET.upper()}})), TARGET)
        encoded = base64.b32encode(bytes.fromhex(TARGET)).decode()
        self.assertEqual(self.submit({'state': True, 'data': {'info_hash': encoded}}), TARGET)

    def test_actual_task_id_preserves_case(self):
        self.assertEqual(self.submit({'state': True, 'data': {'info_hash': TARGET, 'task_id': 'Task-AbC'}}), 'Task-AbC')

    def test_false_envelope_cannot_succeed_by_echo(self):
        self.assert_rejected({'state': False, 'data': {'info_hash': TARGET, 'message': SECRET}}, '115_submit_receipt_rejected')

    def test_nonzero_and_malformed_codes_fail_closed(self):
        for code in ('errno', 'errNo', 'errcode', 'errCode', 'code'):
            for value in (1, '403', 200, 0.0, False, None, SECRET):
                with self.subTest(code=code, value=value):
                    self.assert_rejected({'state': True, code: value, 'data': {'info_hash': TARGET}})
            self.assertEqual(self.submit({'state': True, code: '0', 'data': {'info_hash': TARGET}}), TARGET)

    def test_missing_or_nonboolean_success_marker(self):
        for marker in ({}, {'state': 1}, {'state': 'true'}, {'state': None}):
            with self.subTest(marker=marker):
                self.assert_rejected({**marker, 'data': {'info_hash': TARGET}})

    def test_nested_and_contradictory_receipts(self):
        for extra in ({'success': False}, {'state': False}, {'code': 1}, {'state': 1}):
            with self.subTest(extra=extra):
                self.assert_rejected({'state': True, 'data': {'info_hash': TARGET, **extra}})
        self.assert_rejected({'state': True, 'success': False, 'data': {'info_hash': TARGET}})

    def test_identity_missing_wrong_invalid_or_conflicting(self):
        for data in ({'task_id': '123'}, {'info_hash': 'a' * 40}, {'info_hash': SECRET},
                     {'info_hash': None}, {'info_hash': TARGET, 'infohash': 'a' * 40},
                     {'info_hash': TARGET, 'hash': 'a' * 40}, {'hash': SECRET}):
            with self.subTest(data=data):
                self.assert_rejected({'state': True, 'data': data}, '115_submit_receipt_identity_mismatch')
        self.assert_rejected({'state': True, 'info_hash': 'a' * 40, 'data': {'info_hash': TARGET}})
        self.assert_rejected({'state': True, 'hash': 'a' * 40, 'data': {'info_hash': TARGET}})
        self.assertEqual(self.submit({'state': True, 'data': {'hash': TARGET}}), TARGET)

    def test_malformed_receipts_and_task_ids(self):
        for response in (None, [], 200, SECRET, '{"private":',
                         {'state': True, 'data': SECRET}, {'state': True, 'data': None}):
            with self.subTest(response=response):
                self.assert_rejected(response, '115_submit_receipt_malformed')
        for value in (True, '', 'https://example.invalid/' + SECRET, [SECRET]):
            with self.subTest(value=value):
                self.assert_rejected({'state': True, 'data': {'info_hash': TARGET, 'task_id': value}})
        self.assert_rejected({'state': True, 'id': 123, 'data': {'info_hash': TARGET, 'task_id': 456}})

    def test_rate_limit_classification_is_preserved(self):
        for receipt in ({'state': False, 'errno': 980005},
                        {'state': False, 'errno': 980005, 'data': SECRET},
                        {'state': False, 'error': '405 METHOD NOT ALLOWED ' + SECRET},
                        {'state': True, 'data': {'state': False, 'errNo': '980005'}}):
            with self.subTest(receipt=receipt):
                with self.assertRaises(RemoteRateLimited) as raised:
                    self.submit(receipt)
                self.assertEqual(str(raised.exception), '115_submit_rate_limited')

    def test_transport_errors_never_echo_raw_exception(self):
        for original, expected in ((TimeoutError(SECRET), TimeoutError),
                                   (RuntimeError(SECRET), RemoteInventoryIncomplete),
                                   (RemoteRateLimited(SECRET), RemoteRateLimited)):
            with self.subTest(kind=type(original).__name__):
                client = ReceiptClient(failure=original)
                with self.assertRaises(expected) as raised:
                    adapter(client).submit(TARGET)
                self.assertNotIn(SECRET, str(raised.exception))
                self.assertTrue(raised.exception.__suppress_context__)
                self.assertEqual(len(client.calls), 1)


class TaskCompletionTests(unittest.TestCase):
    def records(self, pages):
        class Client:
            def clouddownload_task_list(self, payload):
                return pages[payload['page']]
        return list(adapter(Client(), page_size=2, max_task_pages=3)._iter_task_records())

    def test_invalid_task_elements_never_become_complete_inventory(self):
        for invalid in (None, 'unknown', 0, []):
            with self.subTest(invalid=invalid):
                with self.assertRaises(RemoteInventoryIncomplete):
                    self.records({1: {'tasks': [invalid], 'total': 1}})

    def test_short_empty_or_explicit_end_before_total_is_incomplete(self):
        for page in ({'tasks': [{'info_hash': TARGET}], 'total': 2},
                     {'tasks': [], 'total': 2},
                     {'tasks': [{'info_hash': TARGET}, {'id': 2}], 'total': 3, 'has_more': False}):
            with self.subTest(page=page):
                with self.assertRaises(RemoteInventoryIncomplete):
                    self.records({1: page})
        with self.assertRaises(RemoteInventoryIncomplete):
            self.records({1: {'tasks': [{'id': 1}, {'id': 2}], 'total': 3},
                          2: {'tasks': [], 'total': 3}})

    def test_exact_total_empty_total_zero_and_unbounded_short_page(self):
        self.assertEqual(self.records({1: {'tasks': [], 'total': 0}}), [])
        self.assertEqual(len(self.records({1: {'tasks': [{'id': 1}, {'id': 2}], 'total': 3},
                                          2: {'tasks': [{'id': 3}], 'total': 3}})), 3)
        self.assertEqual(len(self.records({1: {'tasks': [{'id': 1}]}})), 1)

    def test_total_change_and_excess_items_are_incomplete(self):
        with self.assertRaises(RemoteInventoryIncomplete):
            self.records({1: {'tasks': [{'id': 1}, {'id': 2}], 'total': 3},
                          2: {'tasks': [{'id': 3}], 'total': 4}})
        with self.assertRaises(RemoteInventoryIncomplete):
            self.records({1: {'tasks': [{'id': 1}], 'total': 0}})

    def test_all_top_and_nested_totals_must_agree(self):
        for page in ({'total': 1, 'data': {'total': 2, 'tasks': [{'id': 1}]}},
                     {'total': 1, 'total_count': 2, 'tasks': [{'id': 1}]},
                     {'tasks': [{'id': 1}], 'total': 1, 'data': {'total': 2}},
                     {'tasks': [], 'data': {'total': True}}):
            with self.subTest(page=page):
                with self.assertRaises(RemoteInventoryIncomplete):
                    self.records({1: page})
        self.assertEqual(len(self.records({1: {'total': 1, 'data': {'total': '1', 'tasks': [{'id': 1}]}}})), 1)

    def test_continuation_end_and_empty_page_contradictions(self):
        for page in ({'tasks': [], 'has_more': True},
                     {'tasks': [{'id': 1}], 'has_more': True, 'complete': True},
                     {'tasks': [{'id': 1}], 'total': 1, 'has_more': True},
                     {'tasks': [{'id': 1}], 'has_more': True, 'data': {'is_last': True}},
                     {'tasks': [], 'hasMore': False, 'finished': False},
                     {'tasks': [], 'complete': 'true'}):
            with self.subTest(page=page):
                with self.assertRaises(RemoteInventoryIncomplete):
                    self.records({1: page})

    def test_short_page_with_explicit_continuation_fetches_next_page(self):
        self.assertEqual(len(self.records({
            1: {'tasks': [{'id': 1}], 'has_more': True},
            2: {'data': {'tasks': [{'id': 2}], 'has_more': False}},
        })), 2)
        self.assertEqual(len(self.records({
            1: {'total': 2, 'data': {'tasks': [{'id': 1}], 'more': True}},
            2: {'total': 2, 'tasks': [{'id': 2}], 'complete': True},
        })), 2)


class InstalledSdkPaginationTests(unittest.TestCase):
    def pages(self):
        return {page: {'state': True, 'total': 3000, 'count': 399,
                       'page_count': 14, 'page_size': 30, 'page': page,
                       'tasks': [{'id': i} for i in range((page - 1) * 30, min(page * 30, 399))]}
                for page in range(1, 15)}

    def records(self, pages):
        class Client:
            def __init__(self):
                self.requested_pages = []

            def clouddownload_task_list(self, payload):
                self.requested_pages.append(payload['page'])
                return pages[payload['page']]
        client = Client()
        records = list(adapter(client, page_size=30, max_task_pages=15)._iter_task_records())
        return records, client.requested_pages

    def test_real_count_399_capacity_3000_completes_fourteen_pages(self):
        records, pages = self.records(self.pages())
        self.assertEqual(len(records), 399)
        self.assertEqual(pages, list(range(1, 15)))
        self.assertEqual({record['id'] for record in records}, set(range(399)))

    def test_nested_sdk_envelope_uses_same_verified_contract(self):
        records, _ = self.records({page: {'state': True, 'data': value}
                                  for page, value in self.pages().items()})
        self.assertEqual(len(records), 399)

    def test_sdk_metadata_mismatch_is_incomplete(self):
        for key, value in (('count', 398), ('page_count', 13), ('page', 2),
                           ('page_size', 29), ('page_size', 0), ('count', True)):
            with self.subTest(key=key, value=value):
                pages = self.pages()
                pages[2][key] = value
                # The requested-page mismatch must differ from page two.
                if key == 'page':
                    pages[2][key] = 1
                with self.assertRaises(RemoteInventoryIncomplete):
                    self.records(pages)
        for key in ('count', 'page_count', 'page', 'page_size'):
            with self.subTest(missing=key):
                pages = self.pages()
                del pages[1][key]
                with self.assertRaises(RemoteInventoryIncomplete):
                    self.records(pages)

    def test_sdk_page_length_and_end_flags_must_agree(self):
        for page, key, value in ((1, 'tasks', []), (1, 'has_more', False),
                                 (14, 'has_more', True), (14, 'tasks', [{'id': 1}])):
            with self.subTest(page=page, key=key):
                pages = self.pages()
                pages[page][key] = value
                with self.assertRaises(RemoteInventoryIncomplete):
                    self.records(pages)

    def test_sdk_count_must_agree_with_generic_nested_total(self):
        pages = self.pages()
        pages[1]['data'] = {'total': 400}
        with self.assertRaises(RemoteInventoryIncomplete):
            self.records(pages)


if __name__ == '__main__':
    unittest.main()
