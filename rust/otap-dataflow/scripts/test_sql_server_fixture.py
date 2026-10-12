#!/usr/bin/env python3
# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for fixture prerequisite discovery; no database or Docker required."""

import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location(
    "sql_server_fixture", Path(__file__).with_name("sql-server-fixture.py")
)
FIXTURE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FIXTURE)


class PrerequisiteTests(unittest.TestCase):
    # Scenario: OpenSSL is explicitly available on the user's PATH.
    # Guarantees: The user's selected executable wins over Git installation discovery.
    def test_path_executable_takes_precedence(self):
        with mock.patch.object(FIXTURE.shutil, "which", return_value="selected-openssl") as which:
            self.assertEqual(FIXTURE.find_executable("openssl"), "selected-openssl")
            which.assert_called_once_with("openssl")

    # Scenario: OpenSSL is absent from PATH but bundled in the Program Files Git installation.
    # Guarantees: Windows users can run the fixture without installing another OpenSSL package.
    def test_finds_git_openssl_without_path_entry(self):
        with tempfile.TemporaryDirectory() as temporary:
            executable = Path(temporary) / "Git" / "mingw64" / "bin" / "openssl.exe"
            executable.parent.mkdir(parents=True)
            executable.touch()
            with (
                mock.patch.object(FIXTURE.sys, "platform", "win32"),
                mock.patch.object(FIXTURE.shutil, "which", return_value=None),
                mock.patch.dict(os.environ, {"ProgramFiles": temporary}, clear=True),
            ):
                self.assertEqual(FIXTURE.find_executable("openssl"), str(executable))

    # Scenario: Git is installed outside the default Windows installation paths.
    # Guarantees: OpenSSL can be located relative to the discovered Git executable.
    def test_finds_openssl_relative_to_git(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            executable = root / "usr" / "bin" / "openssl.exe"
            executable.parent.mkdir(parents=True)
            executable.touch()
            with (
                mock.patch.object(FIXTURE.sys, "platform", "win32"),
                mock.patch.object(
                    FIXTURE.shutil, "which",
                    side_effect=lambda name: str(root / "cmd" / "git.exe") if name == "git" else None,
                ),
                mock.patch.dict(os.environ, {}, clear=True),
            ):
                self.assertEqual(FIXTURE.find_executable("openssl"), str(executable))

    # Scenario: OpenSSL is absent from both PATH and the supported installation locations.
    # Guarantees: Setup names the missing prerequisite before creating a partial fixture directory.
    def test_missing_openssl_fails_before_creating_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "fixture"
            with (
                mock.patch.object(FIXTURE.sys, "platform", "linux"),
                mock.patch.object(FIXTURE.shutil, "which", return_value=None),
                mock.patch.object(FIXTURE.subprocess, "run") as run,
            ):
                with self.assertRaisesRegex(RuntimeError, "Required executable 'openssl'"):
                    FIXTURE.start(target)
                self.assertFalse(target.exists())
                run.assert_not_called()

    # Scenario: OpenSSL is available but Docker's executable is not.
    # Guarantees: Setup reports Docker explicitly without producing certificates or a directory.
    def test_missing_docker_fails_before_creating_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "fixture"
            with (
                mock.patch.object(FIXTURE.shutil, "which", side_effect=lambda name: name if name == "openssl" else None),
                mock.patch.object(FIXTURE.subprocess, "run") as run,
            ):
                with self.assertRaisesRegex(RuntimeError, "Required executable 'docker'"):
                    FIXTURE.start(target)
                self.assertFalse(target.exists())
                run.assert_not_called()

    # Scenario: Docker is installed but its daemon is stopped or it uses Windows containers.
    # Guarantees: Setup reports an actionable Linux-container prerequisite before writing files.
    def test_unavailable_or_wrong_mode_docker_fails_preflight(self):
        for result in [
            subprocess.CompletedProcess(["docker"], 1, "", "daemon unavailable"),
            subprocess.CompletedProcess(["docker"], 0, "windows\n", ""),
        ]:
            with self.subTest(returncode=result.returncode, output=result.stdout):
                with tempfile.TemporaryDirectory() as temporary:
                    target = Path(temporary) / "fixture"
                    with (
                        mock.patch.object(FIXTURE.shutil, "which", side_effect=lambda name: name),
                        mock.patch.object(FIXTURE.subprocess, "run", return_value=result),
                    ):
                        with self.assertRaisesRegex(RuntimeError, "Linux-container mode"):
                            FIXTURE.start(target)
                        self.assertFalse(target.exists())

    # Scenario: A subprocess executable disappears between preflight and execution.
    # Guarantees: The error identifies the executable without including secret command arguments.
    def test_missing_subprocess_diagnostic_redacts_arguments(self):
        with mock.patch.object(FIXTURE.subprocess, "run", side_effect=FileNotFoundError):
            with self.assertRaises(RuntimeError) as raised:
                FIXTURE.run(["openssl", "SENSITIVE-argument"])
        self.assertIn("'openssl'", str(raised.exception))
        self.assertNotIn("SENSITIVE", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
