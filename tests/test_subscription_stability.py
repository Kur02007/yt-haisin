"""Offline regression tests: fake credentials, in-memory DB, no external requests."""
import os
os.environ.update(DATABASE_URL="sqlite://", CELERY_BROKER_URL="memory://",
                  DISCORD_BOT_TOKEN="test", YOUTUBE_WEBHOOK_SECRET="test-secret",
                  CALLBACK_URL="https://example.invalid/webhook/youtube")
import logging
import sys
import unittest
from collections import Counter
from datetime import timedelta
from types import ModuleType
from unittest.mock import Mock, patch
from urllib.parse import urlencode

import requests
from starlette.requests import Request
from models import Base, Broadcaster, SessionLocal, engine
import subscription_tasks as s
from config import Config

# Webhook tests exercise the actual route, without importing unrelated worker code.
tasks_stub = ModuleType('tasks')
tasks_stub.process_livestream_task = Mock()
sys.modules['tasks'] = tasks_stub
import main

CHANNEL = 'UC' + 'a' * 22


class SubscriptionTests(unittest.TestCase):
    def setUp(self):
        self.network = patch('requests.request', side_effect=AssertionError('Unexpected real network request'))
        self.network.start()
        self.now = s.utcnow()
        self.clock = patch.object(s, 'utcnow', return_value=self.now)
        self.clock.start()
        self.b = Broadcaster(id=1, channel_id=CHANNEL, is_active=True,
            subscription_state='verified', subscription_expires_at=self.now + timedelta(hours=16),
            subscription_failure_count=0, subscription_retry_count=0, subscription_verify_count=0,
            subscription_renewal_state='idle', subscription_blocked=False)
        self.metrics = Counter()

    def tearDown(self):
        self.clock.stop()
        self.network.stop()

    def run_one(self, status=None, post=None):
        if status is None:
            status = ('verified', self.b.subscription_expires_at)
        with patch.object(s, '_fetch_hub_status', side_effect=status if isinstance(status, Exception) else None,
                          return_value=status) as get, patch.object(s, '_post_subscribe', side_effect=post) as send:
            s._process_one(self.b, self.metrics)
            return get, send

    def test_ample_lease_skips_post(self):
        self.b.subscription_expires_at = self.now + timedelta(days=6)
        _, send = self.run_one()
        send.assert_not_called()
        self.assertEqual(self.metrics['maintained'], 1)

    def test_five_day_boundary_renews(self):
        self.b.subscription_expires_at = self.now + timedelta(days=5)
        _, send = self.run_one()
        send.assert_called_once()

    def test_accepted_is_pending_not_success(self):
        old = self.b.subscription_expires_at
        self.run_one()
        self.assertEqual(self.b.subscription_expires_at, old)
        self.assertEqual(self.b.subscription_state, 'verified')
        self.assertEqual(self.b.subscription_renewal_state, 'pending')
        self.assertEqual(self.metrics['accepted'], 1)
        self.assertEqual(self.metrics['extended'], 0)
        self.assertIsNotNone(self.b.last_subscribed_at)

    def test_same_expiry_verification_never_reposts(self):
        self.run_one()
        for _ in range(3):
            _, send = self.run_one()
            send.assert_not_called()
        self.assertEqual(self.metrics['extended'], 0)
        self.assertIsNone(self.b.subscription_next_check_at)
        self.assertEqual(self.b.subscription_renewal_state, 'pending')

    def test_extension_is_only_success_path(self):
        self.run_one()
        self.b.subscription_failure_count = 3
        _, send = self.run_one(('verified', self.now + timedelta(days=10)))
        send.assert_not_called()
        self.assertEqual(self.metrics['extended'], 1)
        self.assertEqual(self.b.subscription_failure_count, 0)
        self.assertIsNone(self.b.subscription_pending_since)
        self.assertIsNotNone(self.b.subscription_renewed_at)

    def test_unknown_expiry_initial_request_uses_acceptance_baseline(self):
        self.b.subscription_expires_at = None
        self.run_one(('unknown', None))
        self.assertEqual(self.b.subscription_renewal_baseline_at, self.now)
        self.run_one(('verified', self.now + timedelta(days=10)))
        self.assertEqual(self.metrics['extended'], 1)

    def test_503_keeps_valid_lease_and_retries_bounded_with_jitter(self):
        expires = self.b.subscription_expires_at
        for index, base in enumerate((30, 90, 300)):
            self.run_one(post=s.HubError('request_http_503', True))
            delay = (self.b.subscription_next_check_at - self.now).total_seconds()
            self.assertGreaterEqual(delay, base * 0.8)
            self.assertLessEqual(delay, base * 1.2)
            self.assertEqual(self.b.subscription_retry_count, index + 1)
        self.run_one(post=s.HubError('request_http_503', True))
        self.assertIsNone(self.b.subscription_next_check_at)
        self.assertEqual(self.b.subscription_expires_at, expires)
        self.assertEqual(self.b.subscription_state, 'verified')
        self.assertEqual(self.metrics['attempted'], 4)
        self.assertEqual(self.metrics['transient_errors'], 4)
        self.assertEqual(self.b.subscription_failure_count, 4)

    def test_4xx_is_blocked_no_retry_even_next_cycle(self):
        self.run_one(post=s.HubError('request_http_400'))
        self.assertTrue(self.b.subscription_blocked)
        self.assertIsNone(self.b.subscription_next_check_at)
        _, send = self.run_one()
        send.assert_not_called()

    def test_get_4xx_never_posts(self):
        _, send = self.run_one(s.HubError('status_http_403'))
        send.assert_not_called()
        self.assertTrue(self.b.subscription_blocked)

    def test_get_failure_preserves_lease_and_attempts_renewal(self):
        expiry = self.now + timedelta(days=6)
        self.b.subscription_expires_at = expiry
        _, send = self.run_one(s.HubError('network_timeout', True))
        send.assert_called_once()
        self.assertEqual(self.b.subscription_expires_at, expiry)
        self.assertIsNone(self.b.subscription_checked_at)
        self.assertEqual(self.b.subscription_last_error, 'status_network_timeout')

    def test_previous_query_failure_forces_post_with_ample_lease(self):
        self.b.subscription_expires_at = self.now + timedelta(days=6)
        self.b.subscription_last_error = 'status_network_timeout'
        _, send = self.run_one()
        send.assert_called_once()

    def test_verify_query_failure_does_not_repost(self):
        self.run_one()
        _, send = self.run_one(s.HubError('network_timeout', True))
        send.assert_not_called()
        self.assertEqual(self.b.subscription_next_action, 'verify')

    def test_expired_lease_is_critical_even_after_503(self):
        self.b.subscription_expires_at = self.now - timedelta(seconds=1)
        self.run_one(post=s.HubError('request_http_503', True))
        self.assertEqual(self.b.subscription_state, 'expired')
        self.assertTrue(s._urgent(self.b, self.now))

    def test_only_short_lease_is_urgent(self):
        self.assertFalse(s._urgent(self.b, self.now))
        self.b.subscription_expires_at = self.now + timedelta(hours=6)
        self.assertTrue(s._urgent(self.b, self.now))

    def test_parse_real_style_html_and_timezone(self):
        state, expires = s._parse_status('<td>State</td><td>verified</td><td>Expiration time</td><td>Sun, 20 Sep 2026 19:07:02 GMT</td>')
        self.assertEqual(state, 'verified')
        self.assertEqual(expires.isoformat(), '2026-09-20T19:07:02+00:00')
        with self.assertRaises(s.HubError):
            s._parse_status('<h1>Secret error details</h1>')

    def test_missing_expiry_is_not_validity(self):
        state, exp = s._parse_status('<td>State</td><td>verified</td><td>Expiration</td><td>unknown</td>')
        self.assertIsNone(exp)
        _, send = self.run_one((state, exp))
        send.assert_called_once()

    def test_requests_do_not_leak_secrets_in_exception(self):
        with patch('requests.request', side_effect=requests.Timeout('https://secret.invalid/?channel_id=PRIVATE&secret=SECRET')):
            with self.assertRaises(s.HubError) as raised:
                s._fetch_hub_status(CHANNEL)
        self.assertEqual(str(raised.exception), 'network_timeout')

    def test_actual_post_status_handling_and_no_redirects(self):
        for code in (202, 204, 400, 429, 503):
            response = Mock(status_code=code, text='SECRET')
            with patch('requests.request', return_value=response) as request:
                if code in (202, 204):
                    s._post_subscribe(CHANNEL)
                else:
                    with self.assertRaises(s.HubError) as raised:
                        s._post_subscribe(CHANNEL)
                    self.assertEqual(raised.exception.transient, code == 503)
                    self.assertNotIn('SECRET', str(raised.exception))
                self.assertFalse(request.call_args.kwargs['allow_redirects'])
                response.close.assert_called_once()


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        Base.metadata.create_all(engine)
        self.now = s.utcnow()
        self.patches = [patch.object(s, 'utcnow', return_value=self.now),
            patch.object(s, 'engine'), patch.object(s.time, 'sleep'),
            patch.object(s.process_due_subscriptions, 'apply_async'),
            patch.object(s, '_fetch_hub_status', return_value=('verified', self.now + timedelta(days=6))),
            patch.object(s, '_post_subscribe')]
        self.mocks = [p.start() for p in self.patches]
        self.mocks[1].connect.return_value.__enter__.return_value.execute.return_value.scalar.return_value = True
        with SessionLocal() as db:
            for i, hours in enumerate((128, 15, 5)):
                db.add(Broadcaster(channel_id='UC' + str(i) * 22, subscription_state='verified',
                    subscription_expires_at=self.now + timedelta(hours=hours)))
            db.commit()

    def tearDown(self):
        for p in reversed(self.patches):
            p.stop()
        Base.metadata.drop_all(engine)

    def test_hourly_dispatch_and_durable_due_processing(self):
        s._run('hourly')
        self.assertEqual(self.mocks[4].call_count, 1)
        with SessionLocal() as db:
            self.assertEqual(db.query(Broadcaster).filter(Broadcaster.subscription_next_check_at.isnot(None)).count(), 2)
        s._run()
        s._run()
        self.assertEqual(self.mocks[4].call_count, 3)
        self.mocks[5].assert_not_called()

    def test_urgent_dispatch_only_short_lease(self):
        s._run('urgent')
        self.mocks[4].assert_called_once_with('UC' + '2' * 22)

    def test_repeat_hourly_does_not_start_duplicate_cycle(self):
        s._run('hourly')
        s._run()
        s._run()
        s._run('hourly')
        self.assertEqual(self.mocks[4].call_count, 3)

    def test_pending_retry_is_not_overwritten_by_hourly(self):
        with SessionLocal() as db:
            b = db.get(Broadcaster, 1)
            b.subscription_next_check_at = self.now + timedelta(minutes=4)
            b.subscription_next_action = 'retry'
            b.subscription_retry_count = 2
            db.commit()
        s._run('hourly')
        with SessionLocal() as db:
            b = db.get(Broadcaster, 1)
            self.assertEqual(b.subscription_next_action, 'retry')
            self.assertEqual(b.subscription_retry_count, 2)

    def test_next_hour_not_suppressed_by_late_retry(self):
        with SessionLocal() as db:
            b = db.get(Broadcaster, 1)
            b.subscription_cycle_started_at = self.now - timedelta(hours=1)
            b.subscription_last_attempt_at = self.now - timedelta(minutes=50)
            db.commit()
        s._run('hourly')
        self.mocks[4].assert_called_once_with('UC' + '0' * 22)

    def test_idle_sweep_does_not_enqueue_duplicate_eta(self):
        with SessionLocal() as db:
            db.get(Broadcaster, 1).subscription_next_check_at = self.now + timedelta(minutes=2)
            db.commit()
        s._run()
        self.mocks[3].assert_not_called()
        self.mocks[4].assert_not_called()

    def test_denied_record_is_terminal_and_preserves_known_expiry(self):
        s.record_subscription_denial.run(1, 'denied_callback')
        with SessionLocal() as db:
            b = db.get(Broadcaster, 1)
            self.assertEqual(b.subscription_state, 'denied')
            self.assertTrue(b.subscription_blocked)
            self.assertIsNotNone(b.subscription_expires_at)
            self.assertEqual(b.subscription_last_error, 'denied_callback')

    def test_lock_contention_defers_initial_without_http(self):
        self.mocks[1].connect.return_value.__enter__.return_value.execute.return_value.scalar.return_value = False
        with patch.object(s.request_initial_subscription, 'apply_async') as enqueue:
            self.assertEqual(s._run('initial', 1), {'busy': 1})
            enqueue.assert_called_once()
        self.mocks[4].assert_not_called()


class WebhookTests(unittest.TestCase):
    def setUp(self):
        Base.metadata.create_all(engine)
        with SessionLocal() as db:
            db.add(Broadcaster(channel_id=CHANNEL, is_active=True))
            db.commit()

    def tearDown(self):
        Base.metadata.drop_all(engine)

    def invoke(self, **values):
        params = {'hub.mode': 'subscribe', 'hub.topic': s.topic_url(CHANNEL), 'hub.challenge': '<script>&Hello+?=日本語'}
        params.update(values)
        params = {key: value for key, value in params.items() if value is not None}
        return main.verify_subscription(Request({'type': 'http', 'query_string': urlencode(params).encode()}))

    def test_challenge_exact_plain_text_nosniff(self):
        response = self.invoke()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.body.decode(), '<script>&Hello+?=日本語')
        self.assertTrue(response.headers['content-type'].startswith('text/plain'))
        self.assertEqual(response.headers['x-content-type-options'], 'nosniff')

    def test_unsubscribe_is_supported(self):
        self.assertEqual(self.invoke(**{'hub.mode': 'unsubscribe'}).status_code, 200)

    def test_unknown_channel_bad_topic_mode_and_missing_challenge(self):
        for values in ({'hub.mode': 'evil'}, {'hub.challenge': None},
                       {'hub.topic': s.topic_url(CHANNEL) + '&extra=1'},
                       {'hub.topic': s.topic_url('UC' + 'b' * 22)},
                       {'hub.topic': s.topic_url(CHANNEL).replace('https:', 'http:')},
                       {'hub.topic': s.topic_url(CHANNEL).replace('youtube.com', 'youtube.com.evil')}):
            self.assertEqual(self.invoke(**values).status_code, 404)

    def test_duplicate_params_are_rejected(self):
        query = urlencode({'hub.mode': 'subscribe', 'hub.topic': s.topic_url(CHANNEL), 'hub.challenge': 'x'}) + '&hub.mode=unsubscribe'
        response = main.verify_subscription(Request({'type': 'http', 'query_string': query.encode()}))
        self.assertEqual(response.status_code, 404)

    def test_denial_queues_only_sanitized_category(self):
        with patch.object(main.record_subscription_denial, 'delay') as enqueue:
            response = self.invoke(**{'hub.mode': 'denied', 'hub.challenge': None,
                'hub.reason': 'verification failed https://example.invalid?secret=LEAK&channel_id=PRIVATE'})
            self.assertEqual(response.status_code, 204)
            self.assertEqual(enqueue.call_args.args, (1, 'denied_verification'))

    def test_access_log_has_no_query(self):
        record = logging.LogRecord('uvicorn.access', logging.INFO, '', 1, '%s %s %s %s %s',
            ('client', 'GET', '/webhook/youtube?hub.secret=SECRET&channel_id=PRIVATE', '1.1', 200), None)
        main.WebhookAccessLogFilter().filter(record)
        self.assertNotIn('SECRET', record.getMessage())
        self.assertNotIn('PRIVATE', record.getMessage())

    def test_schedule_retains_45_second_polling(self):
        schedule = s.celery_app.conf.beat_schedule
        self.assertEqual(schedule['poll-live-status']['schedule'].total_seconds(), 45)
        self.assertEqual(schedule['renew-subscriptions']['schedule'].minute, {7})
        self.assertEqual(schedule['urgent-subscriptions']['schedule'].total_seconds(), 1200)


if __name__ == '__main__':
    unittest.main(verbosity=2)
