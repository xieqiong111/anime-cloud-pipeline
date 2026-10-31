import contextlib
import io
import json
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

import run_pipeline as cli
from pipeline.config import ConfigSchema
from pipeline.paths import PathMapper
from tests.test_cli import valid_config


class MediaCliTests(unittest.TestCase):
    def test_real_service_factory_inventory_callback_is_bound(self):
        with tempfile.TemporaryDirectory() as directory:
            config=ConfigSchema.from_mapping(valid_config(directory),{},'linux')
            cloud=SimpleNamespace(target_directory_id='source',supports_isolated_move=lambda:False)
            emby=object()
            store=object()
            with mock.patch('pipeline.media_inventory.prove_submission_inventory',return_value={'state':'missing'}) as proof:
                services=cli._build_stage_services(config,moviepilot=object(),cloud=cloud,litepan=object(),
                                                   emby=emby,mapper=PathMapper(config.paths),store=store)
                row={'id':7}
                self.assertEqual(services['download'].pre_submit_inventory(row),{'state':'missing'})
                proof.assert_called_once_with(row,config=config,cloud=cloud,emby=emby,store=store)

    def test_details_project_identity_without_credentials_or_links(self):
        secret='fixture-private-token'
        sub=SimpleNamespace(subscription_id='fixture-subscription',media_source='tmdb',media_id='12345',
                            work='Show',season=1,season_air_date='2026-10-01')
        row={'media_id':'12345','season':1,'episode':4,'title':secret+' https://private.invalid/play',
             'infohash':'a'*40,'source_url':'https://private.invalid/source','torrent_url':'magnet:private',
             'decision':'candidate','reason':'eligible'}
        plan=SimpleNamespace(subscriptions=[sub],decisions=[SimpleNamespace(to_mapping=lambda:row)])
        report=cli._dry_run_details(plan,(secret,))
        encoded=json.dumps(report)
        self.assertNotIn(secret,encoded)
        self.assertNotIn('https://',encoded)
        self.assertNotIn('source_url',encoded)
        self.assertNotIn('torrent_url',encoded)
        self.assertNotIn('a'*40,encoded)
        self.assertEqual(report['decision_details'][0]['episode'],4)

    def test_unknown_inventory_is_a_blocked_cli_result(self):
        with tempfile.TemporaryDirectory() as directory:
            config=ConfigSchema.from_mapping(valid_config(directory),{},'linux')
            runtime=SimpleNamespace(cloud=None, services={'discovery':object(),'candidate_selector':object()},
                                    providers=(),moviepilot=object(),mapper=object())
            stdout=io.StringIO()
            with mock.patch.object(cli,'_config_mapping',return_value=({},config)), \
                 mock.patch.object(cli,'_window',return_value={'season_quarter':'2026-10'}), \
                 mock.patch.object(cli,'_quarter_resource_mismatches',return_value=[]), \
                 mock.patch.object(cli,'build_runtime',return_value=runtime), \
                 mock.patch.object(cli,'_validate_stage_services'), \
                 mock.patch.object(cli,'_open_writable',return_value=object()), \
                 mock.patch.object(cli,'_close_store'), \
                 mock.patch.object(cli,'PipelineRunner') as runner, \
                 contextlib.redirect_stdout(stdout):
                runner.return_value.run.return_value={'errors':0,'stop_reason':'pre_submit_inventory_unknown'}
                code=cli.cmd_run(SimpleNamespace(config='fixture',resume_only=True))
            self.assertEqual(code,2)
            self.assertEqual(json.loads(stdout.getvalue())['status'],'blocked')

    def test_unknown_stop_reason_is_not_echoed(self):
        report=cli._summary_projection({'stop_reason':'fixture-private-error'})
        self.assertEqual(report['stop_reason'],'media_stage_blocked')
