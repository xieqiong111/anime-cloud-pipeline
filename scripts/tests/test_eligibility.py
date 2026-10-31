from __future__ import annotations

import copy
import unittest

from pipeline.eligibility import ALLOWED_USERNAMES, qualify_subscriptions


QUARTER = "2026-07"


def subscription(
    *,
    username="admin",
    media_id=111,
    season=1,
    air_date="2026-07-03",
    genre_ids=(16,),
):
    row = {
        "id": "private-subscription-id",
        "username": username,
        "type": "tv",
        "enabled": True,
        "media_source": "tmdb",
        "media_id": media_id,
        "season": season,
        "name": "private subscription name",
        "tmdb_genre_ids": list(genre_ids) if genre_ids is not None else [],
        "tmdb_seasons": [{"season_number": season, "air_date": air_date}],
        "tmdb_season_air_date": air_date,
    }
    return row


def qualify(rows, *, quarter=QUARTER, allowed=tuple(ALLOWED_USERNAMES)):
    return qualify_subscriptions(rows, quarter=quarter, allowed_usernames=allowed)


class SubscriptionEligibilityTests(unittest.TestCase):
    def test_allowlist_is_exact_case_sensitive_and_has_no_suffix_match(self):
        rows = [
            subscription(username="admin", media_id=1),
            subscription(username="Bangumi订阅", media_id=2),
            subscription(username="Admin", media_id=3, genre_ids=None),
            subscription(username="Bangumi订阅-备份", media_id=4, genre_ids=None),
            subscription(username="admin ", media_id=5, genre_ids=None),
        ]
        for row in rows[2:]:
            row.pop("tmdb_seasons")
            row.pop("tmdb_season_air_date")
        result = qualify(rows)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["counts"]["source_pass"], 2)
        self.assertEqual(result["counts"]["source_excluded"], 3)
        self.assertEqual([item["media_id"] for item in result["eligible"]], ["1", "2"])
        self.assertNotIn("private subscription name", repr(result))
        self.assertNotIn("private-subscription-id", repr(result))

    def test_allowlist_argument_cannot_be_widened_or_changed(self):
        for allowed in (("admin", "Bangumi订阅", "extra"), ("admin", "Admin"),
                        ("admin", ["Bangumi订阅"])):
            with self.subTest(allowed=allowed):
                result = qualify([subscription()], allowed=allowed)
                self.assertEqual(result["status"], "blocked")
                self.assertEqual(result["eligible"], [])
                self.assertIn("source_policy_invalid", self._reasons(result))

    def test_zero_eligible_is_a_successful_noop_when_all_rows_are_known_exclusions(self):
        movie = subscription(media_id=1, genre_ids=(35,))
        other_source = subscription(username="other", media_id=2, genre_ids=None)
        other_source.pop("tmdb_seasons")
        other_source.pop("tmdb_season_air_date")
        result = qualify([movie, other_source])
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["counts"]["eligible"], 0)
        self.assertEqual(result["counts"]["non_anime"], 1)
        self.assertEqual(result["counts"]["source_excluded"], 1)
        self.assertEqual(result["counts"]["unknown"], 0)

    def test_non_anime_is_excluded_but_unknown_genre_blocks(self):
        non_anime = qualify([subscription(genre_ids=(35,))])
        self.assertEqual(non_anime["status"], "ok")
        self.assertEqual(non_anime["counts"]["non_anime"], 1)
        self.assertEqual(non_anime["eligible"], [])

        unknown = qualify([subscription(genre_ids=None)])
        self.assertEqual(unknown["status"], "blocked")
        self.assertEqual(unknown["counts"]["unknown"], 1)
        self.assertIn("anime_genre_unknown", self._reasons(unknown))

    def test_missing_and_mismatched_corresponding_season_dates_block(self):
        missing = subscription()
        missing.pop("tmdb_seasons")
        result_missing = qualify([missing])
        self.assertEqual(result_missing["status"], "blocked")
        self.assertIn("season_date_missing", self._reasons(result_missing))

        mismatch = subscription()
        mismatch["tmdb_seasons"] = [{"season_number": 2, "air_date": "2026-07-03"}]
        result_mismatch = qualify([mismatch])
        self.assertEqual(result_mismatch["status"], "blocked")
        self.assertIn("season_date_missing", self._reasons(result_mismatch))

        conflicting_root = subscription()
        conflicting_root["season_air_date"] = "2026-08-03"
        result_conflict = qualify([conflicting_root])
        self.assertEqual(result_conflict["status"], "blocked")
        self.assertIn("season_date_conflict", self._reasons(result_conflict))

    def test_duplicate_rows_deduplicate_but_conflicting_evidence_blocks(self):
        first = subscription(media_id=10)
        duplicate = copy.deepcopy(first)
        duplicate["id"] = "another-private-id"
        same = qualify([first, duplicate])
        self.assertEqual(same["status"], "ok")
        self.assertEqual(same["counts"]["duplicates"], 1)
        self.assertEqual(same["counts"]["eligible"], 1)
        self.assertEqual(len(same["eligible"]), 1)

        genre_conflict = copy.deepcopy(first)
        genre_conflict["tmdb_genre_ids"] = [35]
        conflicted_genre = qualify([first, genre_conflict])
        self.assertEqual(conflicted_genre["status"], "blocked")
        self.assertIn("duplicate_evidence_conflict", self._reasons(conflicted_genre))
        self.assertEqual(conflicted_genre["eligible"], [])

        date_conflict = subscription(media_id=10, air_date="2026-08-03")
        conflicted_date = qualify([first, date_conflict])
        self.assertEqual(conflicted_date["status"], "blocked")
        self.assertIn("duplicate_evidence_conflict", self._reasons(conflicted_date))
        self.assertEqual(conflicted_date["eligible"], [])

    def test_detail_evidence_without_root_date_is_sufficient(self):
        row = subscription()
        row.pop("tmdb_season_air_date")
        result = qualify([row])
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["counts"]["eligible"], 1)

    def test_malformed_root_and_conflicting_detail_date_aliases_block(self):
        malformed = subscription()
        malformed["season_air_date"] = "not-a-date"
        result = qualify([malformed])
        self.assertEqual(result["status"], "blocked")
        self.assertIn("season_date_missing", self._reasons(result))

        conflict = subscription()
        conflict["tmdb_seasons"][0]["season_air_date"] = "2026-08-03"
        result = qualify([conflict])
        self.assertEqual(result["status"], "blocked")
        self.assertIn("season_date_conflict", self._reasons(result))

    def test_quarter_uses_verified_season_premiere_across_year_boundary(self):
        december = subscription(air_date="2026-12-31")
        january = subscription(media_id=112, air_date="2027-01-01")
        october = qualify([december, january], quarter="2026-10")
        self.assertEqual(october["status"], "ok")
        self.assertEqual([item["media_id"] for item in october["eligible"]], ["111"])
        self.assertEqual(october["counts"]["cross_quarter"], 1)

        january_quarter = qualify([december, january], quarter="2027-01")
        self.assertEqual(january_quarter["status"], "ok")
        self.assertEqual([item["media_id"] for item in january_quarter["eligible"]], ["112"])
        self.assertEqual(january_quarter["counts"]["cross_quarter"], 1)

    def test_one_unknown_blocks_even_when_another_row_qualifies(self):
        valid = subscription(media_id=1)
        unknown = subscription(media_id=2, genre_ids=None)
        result = qualify([valid, unknown])
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["counts"]["eligible"], 1)
        self.assertEqual(result["eligible"][0]["media_id"], "1")
        self.assertEqual(result["counts"]["unknown"], 1)

    def test_missing_username_and_missing_type_or_status_are_unknown(self):
        missing_username = subscription()
        missing_username.pop("username")
        missing_type = subscription(media_id=2)
        missing_type.pop("type")
        missing_status = subscription(media_id=3)
        missing_status.pop("enabled")
        result = qualify([missing_username, missing_type, missing_status])
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["counts"]["unknown"], 3)
        reasons = self._reasons(result)
        self.assertIn("source_username_unknown", reasons)
        self.assertIn("subscription_type_unknown", reasons)
        self.assertIn("subscription_status_unknown", reasons)

    def test_explicit_disablement_precedes_missing_username_and_wrong_source(self):
        disabled_missing_source = {"enabled": False}
        disabled_wrong_source = {"username": "other", "status": "disabled"}
        result = qualify([disabled_missing_source, disabled_wrong_source])
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["counts"]["disabled"], 2)
        self.assertEqual(result["counts"]["unknown"], 0)

    def test_moviepilot_state_codes_prove_active_pause_or_unknown(self):
        active_rows = []
        for state, media_id in (("N", 1), ("R", 2)):
            row = subscription(media_id=media_id)
            row.pop("enabled")
            row["state"] = state
            active_rows.append(row)
        active = qualify(active_rows)
        self.assertEqual(active["status"], "ok")
        self.assertEqual(active["counts"]["eligible"], 2)

        paused = subscription()
        paused["state"] = "S"
        paused_result = qualify([paused])
        self.assertEqual(paused_result["status"], "ok")
        self.assertEqual(paused_result["counts"]["disabled"], 1)
        self.assertEqual(paused_result["counts"]["eligible"], 0)

        pending = subscription()
        pending.pop("enabled")
        pending["state"] = "P"
        pending_result = qualify([pending])
        self.assertEqual(pending_result["status"], "blocked")
        self.assertEqual(pending_result["counts"]["unknown"], 1)

        contradictory = subscription()
        contradictory["state"] = "S"
        contradictory_result = qualify([contradictory])
        self.assertEqual(contradictory_result["status"], "ok")
        self.assertEqual(contradictory_result["counts"]["disabled"], 1)
        self.assertEqual(contradictory_result["counts"]["eligible"], 0)

    def test_api_bad_shape_never_becomes_an_empty_success(self):
        for rows in ({"data": []}, None):
            with self.subTest(rows_type=type(rows).__name__):
                result = qualify(rows)
                self.assertEqual(result["status"], "blocked")
                self.assertEqual(result["eligible"], [])
                self.assertTrue(result["issues"])
                self.assertNotEqual(result["status"], "ok")

        mixed_shape = qualify([subscription(), "bad-row"])
        self.assertEqual(mixed_shape["status"], "blocked")
        self.assertEqual(mixed_shape["counts"]["unknown"], 1)
        self.assertEqual(mixed_shape["counts"]["eligible"], 1)
        self.assertIn("api_row_invalid", self._reasons(mixed_shape))

    @staticmethod
    def _reasons(result):
        return {item["reason"] for item in result["issues"]}


if __name__ == "__main__":
    unittest.main(verbosity=2)
