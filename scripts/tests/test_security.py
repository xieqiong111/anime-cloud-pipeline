"""Focused tests for centralized output sanitization and logging."""
from __future__ import annotations

import io
import logging
import unittest

from pipeline.security import configure_logging, sanitize_text


class SecurityTests(unittest.TestCase):
    def tearDown(self):
        logger = logging.getLogger("pipeline")
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.close()
        logger.filters.clear()
        logger.setLevel(logging.NOTSET)
        logger.propagate = True

    def test_sanitizer_removes_secret_patterns_and_strm_tokens(self):
        value = (
            "Cookie: session=literal-cookie; Authorization: Bearer abc123; "
            "X-API-Key: api-key-value password=letmein credentials=basic; "
            "/t/play-token/n/a.mkv?access_token=qwerty&foo=bar"
        )
        cleaned = sanitize_text(value, secrets=("literal-cookie",))
        for secret in ("literal-cookie", "abc123", "api-key-value", "letmein",
                       "basic", "play-token", "qwerty"):
            self.assertNotIn(secret, cleaned)
        query_cleaned = sanitize_text("/n/a.mkv?access_token=qwerty&foo=bar")
        self.assertNotIn("qwerty", query_cleaned)
        self.assertIn("foo=bar", query_cleaned)

    def test_sanitizer_removes_plain_authorization_and_every_cookie_pair(self):
        cleaned = sanitize_text(
            "Authorization: plain-secret; Cookie: a=first; b=second; path=/"
        )
        for secret in ("plain-secret", "first", "second"):
            self.assertNotIn(secret, cleaned)

    def test_sanitizer_redacts_adjacent_and_equals_form_cookie_headers(self):
        adjacent = sanitize_text(
            "Authorization: plain-secret Cookie: a=first; b=second Request-ID=keep-me"
        )
        self.assertIn("Request-ID=keep-me", adjacent)
        for secret in ("plain-secret", "first", "second"):
            self.assertNotIn(secret, adjacent)

        equals_cookie = sanitize_text("Cookie=first-secret; Request-ID=keep-me")
        self.assertIn("Cookie=[REDACTED]", equals_cookie)
        self.assertIn("Request-ID=keep-me", equals_cookie)
        self.assertNotIn("first-secret", equals_cookie)

        colon_cookie = sanitize_text("Cookie: a=first; b=second; Request-ID=keep-me")
        self.assertIn("Request-ID=keep-me", colon_cookie)
        self.assertNotIn("first", colon_cookie)
        self.assertNotIn("second", colon_cookie)

        set_cookie = sanitize_text("Set-Cookie=second-secret; Request-ID=keep-me")
        self.assertIn("Set-Cookie=[REDACTED]", set_cookie)
        self.assertIn("Request-ID=keep-me", set_cookie)
        self.assertNotIn("second-secret", set_cookie)

    def test_configured_literal_secrets_are_replaced_case_insensitively(self):
        self.assertNotIn("secret-value", sanitize_text("SECRET-VALUE", ["secret-value"]))

    def test_all_project_logger_output_is_filtered(self):
        stream = io.StringIO()
        logger = configure_logging(["cookie-value"], stream)
        logging.getLogger("pipeline.child").warning("cookies=%s", "cookie-value")
        self.assertNotIn("cookie-value", stream.getvalue())
        self.assertIn("[REDACTED]", stream.getvalue())

    def test_logger_does_not_duplicate_handlers_or_bypass_filter(self):
        stream = io.StringIO()
        logger = configure_logging(["token-value"], stream)
        configure_logging(["token-value"], stream)
        child = logging.getLogger("pipeline.child")
        child.addHandler(logging.StreamHandler(io.StringIO()))
        child.error("Authorization: Bearer token-value")
        self.assertEqual(len(logger.handlers), 1)
        self.assertNotIn("token-value", stream.getvalue())

    def test_late_child_handler_cannot_bypass_redaction(self):
        central = io.StringIO()
        child_stream = io.StringIO()
        configure_logging(["late-secret"], central)
        child = logging.getLogger("pipeline.late-child")
        child_handler = logging.StreamHandler(child_stream)
        child.addHandler(child_handler)
        try:
            child.warning("Authorization: late-secret Cookie=late-cookie")
        finally:
            child.removeHandler(child_handler)
            child_handler.close()
        for output in (central.getvalue(), child_stream.getvalue()):
            self.assertNotIn("late-secret", output)
            self.assertNotIn("late-cookie", output)


if __name__ == "__main__":
    unittest.main(verbosity=2)
