"""Unit tests for the deterministic subscription-delivery engine (stdlib only).

Run from backend/:  python3 -I -m unittest tests.test_subscription -v
"""
import copy
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.services.subscription import simulate


def nodes():
    return [
        {'name': 'harbor-primary', 'role': 'primary', 'status': 'healthy',
         'lineage': 'qiyuan-log-01', 'position': 105, 'delay_ms': 0},
        {'name': 'north-replica', 'role': 'replica', 'status': 'healthy',
         'lineage': 'qiyuan-log-01', 'position': 104, 'delay_ms': 150},
        {'name': 'south-replica', 'role': 'replica', 'status': 'healthy',
         'lineage': 'qiyuan-log-01', 'position': 105, 'delay_ms': 20},
    ]


def kinds(result):
    return [(r['kind'], r.get('seq'), r.get('at_ms')) for r in result['timeline']]


class BasicDeliveryTests(unittest.TestCase):
    def test_write_only_committed_when_primary_up(self):
        events = [
            {'at_ms': 0, 'kind': 'write', 'key': 'k1', 'value': 'v1'},
            {'at_ms': 10, 'kind': 'node_down', 'node': 'harbor-primary'},
            {'at_ms': 20, 'kind': 'write', 'key': 'k2', 'value': 'v2'},
        ]
        r = simulate(nodes(), events)
        self.assertEqual(r['summary']['committed'], 1)
        self.assertEqual(r['summary']['failed_writes'], 1)
        self.assertEqual(r['committed_changes'][0]['seq'], 1)
        self.assertEqual(r['committed_changes'][0]['position'], 106)

    def test_subscriber_receives_committed_changes_in_order(self):
        events = [
            {'at_ms': 0, 'kind': 'write', 'key': 'k1', 'value': 'v1'},
            {'at_ms': 5, 'kind': 'subscribe', 'subscriber': 'obs-1'},
            {'at_ms': 500, 'kind': 'ack', 'subscriber': 'obs-1', 'seq': 1},
        ]
        r = simulate(nodes(), events, delivery_ms=50, ack_timeout_ms=300)
        sub = r['subscribers'][0]
        self.assertEqual(sub['acked_seq'], 1)
        self.assertTrue(sub['caught_up'])
        d = [x for x in r['timeline']
             if x['kind'] == 'deliver' and x['arrived'] and not x['duplicate']]
        self.assertEqual(len(d), 1)
        self.assertFalse(d[0]['duplicate'])

    def test_subscription_start_position_respected(self):
        events = [
            {'at_ms': 0, 'kind': 'write', 'key': 'k1', 'value': 'v1'},
            {'at_ms': 10, 'kind': 'write', 'key': 'k2', 'value': 'v2'},
            {'at_ms': 20, 'kind': 'subscribe', 'subscriber': 'obs-1',
             'from_position_seq': 1},
            {'at_ms': 900, 'kind': 'ack', 'subscriber': 'obs-1', 'seq': 2},
        ]
        r = simulate(nodes(), events, delivery_ms=50, ack_timeout_ms=300)
        delivered = [x for x in r['timeline']
                     if x['kind'] == 'deliver' and x['arrived']
                     and not x['duplicate']]
        self.assertEqual([x['seq'] for x in delivered], [2])


class FailoverTests(unittest.TestCase):
    def test_replica_takes_over_after_primary_down_when_caught_up(self):
        events = [
            {'at_ms': 0, 'kind': 'write', 'key': 'k1', 'value': 'v1'},
            {'at_ms': 5, 'kind': 'subscribe', 'subscriber': 'obs-1'},
            {'at_ms': 20, 'kind': 'replicate', 'node': 'south-replica',
             'position': 106, 'delay_ms': 20},
            {'at_ms': 30, 'kind': 'node_down', 'node': 'harbor-primary'},
        ]
        r = simulate(nodes(), events, delivery_ms=50, ack_timeout_ms=300)
        d = [x for x in r['timeline'] if x['kind'] == 'deliver' and x['arrived']]
        self.assertTrue(d)
        self.assertEqual(d[0]['source'], 'south-replica')
        self.assertTrue(r['subscribers'][0]['caught_up'])

    def test_missing_position_waits_until_replica_applies(self):
        events = [
            {'at_ms': 0, 'kind': 'write', 'key': 'k1', 'value': 'v1'},
            {'at_ms': 10, 'kind': 'node_down', 'node': 'harbor-primary'},
            {'at_ms': 20, 'kind': 'subscribe', 'subscriber': 'obs-1'},
            # Replica is at 105, change needs 106: must wait.
            {'at_ms': 500, 'kind': 'replicate', 'node': 'south-replica',
             'position': 106, 'delay_ms': 20},
            {'at_ms': 1200, 'kind': 'ack', 'subscriber': 'obs-1', 'seq': 1},
        ]
        r = simulate(nodes(), events, delivery_ms=50, ack_timeout_ms=300)
        d = [x for x in r['timeline']
             if x['kind'] == 'deliver' and x['arrived'] and not x['duplicate']]
        self.assertEqual(len(d), 1)
        self.assertGreaterEqual(d[0]['at_ms'], 500)
        self.assertEqual(d[0]['source'], 'south-replica')
        self.assertTrue(r['subscribers'][0]['caught_up'])

    def test_no_source_when_nobody_holds_position(self):
        events = [
            {'at_ms': 0, 'kind': 'write', 'key': 'k1', 'value': 'v1'},
            {'at_ms': 10, 'kind': 'node_down', 'node': 'harbor-primary'},
            {'at_ms': 20, 'kind': 'node_down', 'node': 'south-replica'},
            {'at_ms': 30, 'kind': 'subscribe', 'subscriber': 'obs-1'},
        ]
        r = simulate(nodes(), events)
        sub = r['subscribers'][0]
        self.assertFalse(sub['caught_up'])
        self.assertIn('缺位点等待', sub['blocked_reason'])
        self.assertEqual([x for x in r['timeline'] if x['kind'] == 'deliver'], [])


class AckOrderingTests(unittest.TestCase):
    def test_out_of_order_ack_does_not_skip_unconfirmed(self):
        events = [
            {'at_ms': 0, 'kind': 'write', 'key': 'k1', 'value': 'v1'},
            {'at_ms': 5, 'kind': 'write', 'key': 'k2', 'value': 'v2'},
            {'at_ms': 10, 'kind': 'subscribe', 'subscriber': 'obs-1'},
            {'at_ms': 200, 'kind': 'ack', 'subscriber': 'obs-1', 'seq': 2,
             'out_of_order': True},
            {'at_ms': 250, 'kind': 'ack', 'subscriber': 'obs-1', 'seq': 1},
        ]
        r = simulate(nodes(), events, delivery_ms=50, ack_timeout_ms=300,
                     inflight_window=3)
        sub = r['subscribers'][0]
        held = [x for x in r['timeline']
                if x['kind'] == 'ack' and x.get('out_of_order')]
        self.assertEqual([x['seq'] for x in held], [2])
        self.assertEqual(sub['acked_seq'], 2)
        self.assertTrue(sub['caught_up'])
        # After seq1 confirms, the held seq2 drains in the same instant.
        advanced = sorted(x['seq'] for x in r['timeline']
                          if x['kind'] == 'ack' and x.get('advanced'))
        self.assertEqual(advanced, [1, 2])

    def test_cursor_never_passes_unconfirmed_change(self):
        events = [
            {'at_ms': 0, 'kind': 'write', 'key': 'k1', 'value': 'v1'},
            {'at_ms': 5, 'kind': 'write', 'key': 'k2', 'value': 'v2'},
            {'at_ms': 10, 'kind': 'subscribe', 'subscriber': 'obs-1'},
            {'at_ms': 200, 'kind': 'ack', 'subscriber': 'obs-1', 'seq': 2,
             'out_of_order': True},
        ]
        # Window of one: seq2 cannot even be sent while seq1 is unconfirmed.
        # Disable auto re-ack so the held out-of-order ack is observable.
        r = simulate(nodes(), events, delivery_ms=50, ack_timeout_ms=300,
                     inflight_window=1, auto_reack=False)
        self.assertEqual(r['subscribers'][0]['acked_seq'], 0)
        self.assertEqual(r['subscribers'][0]['out_of_order_acks_held'], [2])


class RetryAndIdempotencyTests(unittest.TestCase):
    def test_resend_policy_duplicates_but_catches_up_fast(self):
        events = [
            {'at_ms': 0, 'kind': 'write', 'key': 'k1', 'value': 'v1'},
            {'at_ms': 10, 'kind': 'subscribe', 'subscriber': 'obs-1'},
            {'at_ms': 500, 'kind': 'ack', 'subscriber': 'obs-1', 'seq': 1,
             'lost': True},
        ]
        r = simulate(nodes(), events, policy='resend',
                     delivery_ms=50, ack_timeout_ms=200)
        sub = r['subscribers'][0]
        self.assertTrue(sub['caught_up'])
        self.assertGreaterEqual(sub['duplicate_deliveries'], 1)
        dup = [x for x in r['timeline']
               if x['kind'] == 'deliver' and x['duplicate']]
        self.assertTrue(dup)
        # Applied exactly once despite repeated deliveries.
        applied_once = [x for x in r['timeline']
                        if x['kind'] == 'deliver' and x['applied']]
        self.assertEqual(len(applied_once), 1)
        self.assertTrue(r['summary']['no_double_apply'])

    def test_reconcile_policy_no_duplicates_with_extra_roundtrip(self):
        events = [
            {'at_ms': 0, 'kind': 'write', 'key': 'k1', 'value': 'v1'},
            {'at_ms': 10, 'kind': 'subscribe', 'subscriber': 'obs-1'},
            {'at_ms': 500, 'kind': 'ack', 'subscriber': 'obs-1', 'seq': 1,
             'lost': True},
        ]
        r = simulate(nodes(), events, policy='reconcile',
                     delivery_ms=50, ack_timeout_ms=200, reconcile_rtt_ms=100)
        sub = r['subscribers'][0]
        self.assertTrue(sub['caught_up'])
        self.assertEqual(sub['duplicate_deliveries'], 0)
        self.assertGreaterEqual(r['summary']['reconcile_probes'], 1)
        probes = [x for x in r['timeline'] if x['kind'] == 'probe_send']
        self.assertTrue(probes)

    def test_reconcile_retransmits_when_subscriber_lacked_change(self):
        events = [
            {'at_ms': 0, 'kind': 'write', 'key': 'k1', 'value': 'v1'},
            {'at_ms': 10, 'kind': 'subscribe', 'subscriber': 'obs-1'},
            {'at_ms': 100, 'kind': 'sub_down', 'subscriber': 'obs-1'},
            {'at_ms': 900, 'kind': 'sub_up', 'subscriber': 'obs-1'},
        ]
        r = simulate(nodes(), events, policy='reconcile',
                     delivery_ms=50, ack_timeout_ms=200, reconcile_rtt_ms=100)
        sub = r['subscribers'][0]
        # First delivery never arrived; reconcile probe says "not held", then re-send.
        self.assertTrue(sub['caught_up'])
        self.assertEqual(sub['duplicate_deliveries'], 0)

    def test_resend_is_faster_than_reconcile(self):
        events = [
            {'at_ms': 0, 'kind': 'write', 'key': 'k1', 'value': 'v1'},
            {'at_ms': 10, 'kind': 'subscribe', 'subscriber': 'obs-1'},
            {'at_ms': 500, 'kind': 'ack', 'subscriber': 'obs-1', 'seq': 1,
             'lost': True},
        ]
        fast = simulate(nodes(), events, policy='resend', delivery_ms=50,
                        ack_timeout_ms=200, reconcile_rtt_ms=300)
        careful = simulate(nodes(), events, policy='reconcile', delivery_ms=50,
                           ack_timeout_ms=200, reconcile_rtt_ms=300)
        self.assertLess(fast['subscribers'][0]['catchup_ms'],
                        careful['subscribers'][0]['catchup_ms'])

    def test_subscriber_outage_resumes_without_double_apply(self):
        events = [
            {'at_ms': 0, 'kind': 'write', 'key': 'k1', 'value': 'v1'},
            {'at_ms': 10, 'kind': 'subscribe', 'subscriber': 'obs-1'},
            {'at_ms': 80, 'kind': 'sub_down', 'subscriber': 'obs-1'},
            {'at_ms': 900, 'kind': 'sub_up', 'subscriber': 'obs-1'},
            {'at_ms': 1600, 'kind': 'ack', 'subscriber': 'obs-1', 'seq': 1},
        ]
        r = simulate(nodes(), events, policy='resend',
                     delivery_ms=50, ack_timeout_ms=200)
        sub = r['subscribers'][0]
        self.assertTrue(sub['caught_up'])
        applied = [x for x in r['timeline']
                   if x['kind'] == 'deliver' and x['applied']]
        self.assertEqual(len(applied), 1)


class DeterminismAndValidationTests(unittest.TestCase):
    def test_checksum_stable_for_same_input(self):
        events = [
            {'at_ms': 0, 'kind': 'write', 'key': 'k1', 'value': 'v1'},
            {'at_ms': 10, 'kind': 'subscribe', 'subscriber': 'obs-1'},
            {'at_ms': 800, 'kind': 'ack', 'subscriber': 'obs-1', 'seq': 1},
        ]
        a = simulate(copy.deepcopy(nodes()), events)
        b = simulate(copy.deepcopy(nodes()), events)
        self.assertEqual(a['checksum'], b['checksum'])

    def test_timeline_is_chronological(self):
        events = [
            {'at_ms': 0, 'kind': 'write', 'key': 'k1', 'value': 'v1'},
            {'at_ms': 10, 'kind': 'subscribe', 'subscriber': 'obs-1'},
            {'at_ms': 500, 'kind': 'ack', 'subscriber': 'obs-1', 'seq': 1,
             'lost': True},
            {'at_ms': 1500, 'kind': 'write', 'key': 'k2', 'value': 'v2'},
        ]
        r = simulate(nodes(), events, policy='resend',
                     delivery_ms=50, ack_timeout_ms=200)
        ats = [row['at_ms'] for row in r['timeline']]
        self.assertEqual(ats, sorted(ats))

    def test_validation_errors(self):
        good = [
            {'at_ms': 0, 'kind': 'write', 'key': 'k1', 'value': 'v1'},
        ]
        with self.assertRaises(ValueError):
            simulate(nodes(), good, policy='nope')
        bad_nodes = [dict(nodes()[0])]
        bad_nodes[0]['role'] = 'replica'
        with self.assertRaises(ValueError):
            simulate(bad_nodes, good)
        with self.assertRaises(ValueError):
            simulate(nodes(), [
                {'at_ms': 10, 'kind': 'write'},
                {'at_ms': 0, 'kind': 'write'},
            ])
        with self.assertRaises(ValueError):
            simulate(nodes(), [
                {'at_ms': 0, 'kind': 'ack', 'subscriber': 'obs-1', 'seq': 1},
            ])

    def test_backlog_drains_after_replica_catches_up(self):
        events = [
            {'at_ms': 0, 'kind': 'write', 'key': 'k1', 'value': 'v1'},
            {'at_ms': 10, 'kind': 'write', 'key': 'k2', 'value': 'v2'},
            {'at_ms': 20, 'kind': 'node_down', 'node': 'harbor-primary'},
            {'at_ms': 30, 'kind': 'subscribe', 'subscriber': 'obs-1'},
            {'at_ms': 600, 'kind': 'replicate', 'node': 'south-replica',
             'position': 107, 'delay_ms': 20},
            {'at_ms': 2500, 'kind': 'ack', 'subscriber': 'obs-1', 'seq': 1},
            {'at_ms': 2600, 'kind': 'ack', 'subscriber': 'obs-1', 'seq': 2},
        ]
        r = simulate(nodes(), events, delivery_ms=50, ack_timeout_ms=300,
                     inflight_window=4)
        sub = r['subscribers'][0]
        self.assertTrue(sub['caught_up'])
        delivered = [x for x in r['timeline']
                     if x['kind'] == 'deliver' and x['arrived'] and not x['duplicate']]
        self.assertEqual([x['seq'] for x in delivered], [1, 2])
        self.assertTrue(all(x['at_ms'] >= 600 for x in delivered))


if __name__ == '__main__':
    unittest.main()
