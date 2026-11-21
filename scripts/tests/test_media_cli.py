import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
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
                row={'id':87}
                self.assertEqual(services['download'].pre_submit_inventory(row),{'state':'missing'})
                proof.assert_called_once_with(row,config=config,cloud=cloud,emby=emby,store=store)

    def test_details_project_identity_without_credentials_or_links(self):
        secret='fixture-private-token'
        sub=SimpleNamespace(subscription_id='57',media_source='tmdb',media_id='311842',
                            work='Show',season=1,season_air_date='2026-10-01')
        row={'media_id':'311842','season':1,'episode':4,'title':secret+' https://private.invalid/play',
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


class MediaWrapperTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root=Path(self.tmp.name)
        stub=root/'ssh'
        stub.write_text('#!/bin/sh\nprintf "%s\\n" "$@"\n')
        stub.chmod(0o700)
        self.env={**os.environ,'PATH':str(root)+os.pathsep+os.environ['PATH']}
        self.env.pop('OCTOP_PIPELINE_ALLOW_WRITE',None)
        self.wrapper=Path(__file__).resolve().parents[3]/'candidate/octop-nas-v5.sh'
        if not self.wrapper.exists():
            self.skipTest('deployment wrapper is not part of the public tree')

    def run_wrapper(self,args,authorized=False):
        env={**self.env}
        if authorized:env['OCTOP_PIPELINE_ALLOW_WRITE']='1'
        return subprocess.run(['sh',str(self.wrapper),*args],text=True,capture_output=True,env=env)

    def test_single_episode_fresh_resume_and_readonly_details(self):
        for args in (['run','--backfill','311842:1:4-4'],
                     ['run','--resume-only','--backfill','311842:1:4-4']):
            result=self.run_wrapper(args,True)
            self.assertEqual(result.returncode,0,result.stderr)
            self.assertIn('OCTOP_PIPELINE_ALLOW_WRITE=1',result.stdout)
            self.assertIn(' '.join(args),result.stdout)
        result=self.run_wrapper(['dry-run','--details','--backfill','311842:1:4-4'])
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertNotIn('OCTOP_PIPELINE_ALLOW_WRITE=1',result.stdout)

    def test_invalid_or_unapproved_scope_never_reaches_ssh(self):
        cases=[(['run','--backfill','311842:1:4-4'],False),
               (['run'],True),(['run','--backfill','311842:1:4-5'],True),
               (['run','--backfill','311842:1:4-4;bad'],True),
               (['run','--backfill','311842:1:4-4','--config','other'],True),
               (['dry-run','--resume-only'],False),
               (['dry-run','--details','--backfill','311842:1:0-0'],False)]
        cases += [
            (['dry-run','--backfill ; id #','311842:1:4-4'],False),
            (['run','--backfill ; id #','311842:1:4-4'],True),
            (['run','--resume-only','--backfill ; id #','311842:1:4-4'],True),
            (['dry-run','--details','--backfill ; id #','311842:1:4-4'],False),
            (['dry-run --backfill ; id #','unused','311842:1:4-4'],False),
        ]
        for args,authorized in cases:
            with self.subTest(args=args):
                result=self.run_wrapper(args,authorized)
                self.assertEqual(result.returncode,2)
                self.assertEqual(result.stdout,'')
