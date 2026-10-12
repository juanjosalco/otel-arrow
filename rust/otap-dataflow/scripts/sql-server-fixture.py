#!/usr/bin/env python3
# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Provision an isolated, verified-TLS SQL Server 2022 development fixture."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import time

IMAGE = (
    "mcr.microsoft.com/mssql/server@sha256:"
    "4402d880dd4c34bfa7d8705e56a86cd6c88da80a1f6bbbe741f999e76264a090"
)
LABEL = "otel-arrow.sql-server-fixture"
SQLCMD = "/opt/mssql-tools18/bin/sqlcmd"


def find_executable(name):
    executable = shutil.which(name)
    if executable:
        return executable
    if name == "openssl" and sys.platform == "win32":
        roots = []
        git = shutil.which("git")
        if git:
            roots.append(Path(git).resolve().parent.parent)
        for variable in ("ProgramW6432", "ProgramFiles", "ProgramFiles(x86)"):
            if os.environ.get(variable):
                roots.append(Path(os.environ[variable]) / "Git")
        if os.environ.get("LOCALAPPDATA"):
            roots.append(Path(os.environ["LOCALAPPDATA"]) / "Programs" / "Git")
        for root in roots:
            for relative in (("mingw64", "bin"), ("usr", "bin")):
                candidate = root.joinpath(*relative, "openssl.exe")
                if candidate.is_file():
                    return str(candidate)
    hint = (
        "Use OpenSSL on PATH or the OpenSSL bundled with Git for Windows."
        if name == "openssl"
        else "Install Docker Desktop and make its docker command available on PATH."
    )
    raise RuntimeError(f"Required executable '{name}' was not found. {hint}")


def run(args, *, env=None, timeout=60):
    try:
        result = subprocess.run(
            args, env=env, capture_output=True, text=True, timeout=timeout, check=False
        )
    except FileNotFoundError:
        raise RuntimeError(
            f"Unable to start '{Path(args[0]).name}': executable not found. Check its installation and PATH."
        ) from None
    if result.returncode:
        # Command arguments/output can contain fixture secrets. Keep diagnostics local.
        raise RuntimeError(f"{args[0]} failed with exit code {result.returncode}")
    return result.stdout.strip()


def start(directory):
    openssl = find_executable("openssl")
    docker = find_executable("docker")
    try:
        server_os = run([docker, "info", "--format", "{{.OSType}}"])
    except (RuntimeError, subprocess.TimeoutExpired):
        raise RuntimeError(
            "Docker is unavailable. Start Docker Desktop in Linux-container mode and retry."
        ) from None
    if server_os != "linux":
        raise RuntimeError("The fixture requires Docker in Linux-container mode.")
    directory.mkdir(parents=True, exist_ok=False)
    ca_key = directory / "ca.key"
    ca = directory / "ca.crt"
    key = directory / "server.key"
    csr = directory / "server.csr"
    cert = directory / "server.crt"
    extensions = directory / "server.ext"
    extensions.write_text(
        "basicConstraints=critical,CA:FALSE\n"
        "keyUsage=critical,digitalSignature,keyEncipherment\n"
        "extendedKeyUsage=serverAuth\n"
        "subjectAltName=DNS:localhost\n", encoding="ascii"
    )
    run([openssl, "req", "-x509", "-newkey", "rsa:2048", "-nodes",
         "-keyout", str(ca_key), "-out", str(ca), "-days", "2", "-sha256",
         "-subj", "/CN=OTAP SQL Server fixture CA",
         "-addext", "basicConstraints=critical,CA:TRUE"])
    run([openssl, "req", "-new", "-newkey", "rsa:2048", "-nodes",
         "-keyout", str(key), "-out", str(csr), "-subj", "/CN=localhost"])
    run([openssl, "x509", "-req", "-in", str(csr), "-CA", str(ca),
         "-CAkey", str(ca_key), "-CAcreateserial", "-out", str(cert),
         "-days", "2", "-sha256", "-extfile", str(extensions)])
    (directory / "mssql.conf").write_text(
        "[network]\n"
        "tlscert = /var/opt/mssql/tls/server.crt\n"
        "tlskey = /var/opt/mssql/tls/server.key\n"
        "tlsprotocols = 1.2\n"
        "forceencryption = 1\n", encoding="ascii"
    )
    admin_password = "Aa1!" + secrets.token_urlsafe(30)
    reader_password = "Aa1!" + secrets.token_urlsafe(30)
    password_file = directory / "reader-password.txt"
    password_file.write_text(reader_password, encoding="ascii")
    setup = directory / "setup.sql"
    setup.write_text(
        f"""CREATE LOGIN otel_reader WITH PASSWORD = '{reader_password}', CHECK_POLICY = OFF;
GO
CREATE DATABASE otap_fixture;
GO
USE otap_fixture;
CREATE USER otel_reader FOR LOGIN otel_reader;
CREATE TABLE dbo.OTAP_SQL_EVENTS (
    ID bigint NOT NULL PRIMARY KEY,
    EVENT_TS datetime2(7) NOT NULL,
    PAYLOAD nvarchar(4000) NULL,
    AMOUNT decimal(38,8) NULL,
    RAW varbinary(32) NULL,
    AT_TIME datetimeoffset(7) NULL
);
INSERT INTO dbo.OTAP_SQL_EVENTS VALUES
    (1, '2026-01-01T00:00:01.1234567', N'hello ' + NCHAR(960),
     123456789012345678901234567890.12345678, 0x0001FF,
     '2026-01-01T05:30:00.1234567+05:30'),
    (2, '2026-01-01T00:00:02.1234567', N'second', NULL, NULL, NULL),
    (3, '2026-01-01T00:00:03.1234567', N'third', -1.00000001, 0xAB,
     '2026-01-01T00:00:00-04:00');
CREATE UNIQUE INDEX event_order ON dbo.OTAP_SQL_EVENTS(EVENT_TS, ID);
GRANT SELECT ON dbo.OTAP_SQL_EVENTS TO otel_reader;
GO
""", encoding="ascii"
    )
    owner = hashlib.sha256(str(directory).encode()).hexdigest()
    name = "otel-sql-" + secrets.token_hex(5)
    environment = dict(os.environ, MSSQL_SA_PASSWORD=admin_password)
    startup = (
        "set -eu; "
        "install -d -m 700 -o mssql -g root /var/opt/mssql/tls; "
        "install -m 600 -o mssql -g root /fixture/server.key /var/opt/mssql/tls/server.key; "
        "install -m 644 -o mssql -g root /fixture/server.crt /var/opt/mssql/tls/server.crt; "
        "install -m 644 -o mssql -g root /fixture/mssql.conf /var/opt/mssql/mssql.conf; "
        "install -m 644 /fixture/ca.crt /usr/local/share/ca-certificates/otap-fixture.crt; "
        "update-ca-certificates >/dev/null; "
        "exec runuser -u mssql -- /opt/mssql/bin/sqlservr"
    )
    container = run([
        docker, "run", "--detach", "--name", name,
        "--label", f"{LABEL}={owner}", "--memory", "3g",
        "--publish", "127.0.0.1::1433",
        "--env", "ACCEPT_EULA=Y", "--env", "MSSQL_PID=Developer",
        "--env", "MSSQL_SA_PASSWORD", "--user", "0",
        "--mount", f"type=bind,source={directory},target=/fixture,readonly",
        "--entrypoint", "/bin/bash", IMAGE, "-c", startup,
    ], env=environment, timeout=180)
    metadata = {"container": container, "owner": owner, "image": IMAGE}
    state = directory / "fixture.json"
    state.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    try:
        # Admin setup also verifies the fixture certificate; no sqlcmd -C bypass.
        admin_env = dict(os.environ, SQLCMDPASSWORD=admin_password)
        command = [docker, "exec", "--env", "SQLCMDPASSWORD", container,
                   SQLCMD, "-S", "localhost", "-U", "sa", "-N", "-b", "-l", "2"]
        deadline = time.monotonic() + 120
        while True:
            probe = subprocess.run(command + ["-Q", "SELECT 1"],
                                   env=admin_env, capture_output=True, timeout=10,
                                   check=False)
            if probe.returncode == 0:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("SQL Server did not become ready within 120 seconds")
            time.sleep(1)
        run(command + ["-i", "/fixture/setup.sql"], env=admin_env)
        version = run(command + ["-h", "-1", "-W", "-Q",
                      "SET NOCOUNT ON; SELECT CAST(SERVERPROPERTY('ProductVersion') AS varchar(30))"],
                      env=admin_env)
        if not version.startswith("16."):
            raise RuntimeError("The fixture did not report SQL Server 2022")
        address = run(["docker", "port", container, "1433/tcp"])
        port = int(address.rsplit(":", 1)[1])
        metadata.update({
            "server_version": version,
            "env": {
                "OTAP_SQL_SERVER_HOST": "localhost",
                "OTAP_SQL_SERVER_PORT": str(port),
                "OTAP_SQL_SERVER_DATABASE": "otap_fixture",
                "OTAP_SQL_SERVER_USERNAME": "otel_reader",
                "OTAP_SQL_SERVER_PASSWORD_FILE": str(password_file),
                "OTAP_SQL_SERVER_CA_FILE": str(ca),
            },
        })
        state.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
        # Keep the server key until stop so a deliberate container restart works.
        for filename in ["setup.sql", "ca.key"]:
            (directory / filename).unlink()
        print(str(state))
    except (RuntimeError, subprocess.TimeoutExpired, OSError, ValueError):
        (directory / "server.log").write_text(
            run(["docker", "logs", container]), encoding="utf-8"
        )
        stop(directory)
        raise


def stop(directory):
    metadata = json.loads((directory / "fixture.json").read_text(encoding="utf-8"))
    if metadata.get("stopped"):
        print("Fixture is already stopped")
        return
    identity = run(["docker", "inspect", metadata["container"], "--format",
                    '{{index .Config.Labels "' + LABEL + '"}}'])
    if identity != metadata["owner"]:
        raise RuntimeError("Refusing to remove a container not owned by this fixture")
    (directory / "server.log").write_text(
        run(["docker", "logs", metadata["container"]]), encoding="utf-8"
    )
    run(["docker", "rm", "--force", metadata["container"]])
    for filename in ["reader-password.txt", "setup.sql", "ca.key", "server.key"]:
        path = directory / filename
        if path.exists():
            path.unlink()
    metadata["stopped"] = True
    (directory / "fixture.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    print("Removed fixture container and generated secrets")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["start", "stop"])
    parser.add_argument("--directory", required=True, type=Path)
    args = parser.parse_args()
    directory = args.directory.resolve()
    if args.operation == "start":
        start(directory)
    else:
        stop(directory)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, subprocess.TimeoutExpired, OSError, ValueError) as exc:
        print(f"Fixture failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)
