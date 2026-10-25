#!/usr/bin/env python3
"""ai_lake Kudu 데이터를 정제하여 ai_datawarehouse Kudu로 적재한다.

Python은 데이터를 직접 내려받지 않고 Impala에 SQL을 제출한다. 따라서 실제
대용량 처리(공백 정리, 날짜 표준화, 중복 제거)는 CDP 클러스터에서 수행된다.
"""

import argparse
import json
import logging
import os
import re
import shutil
import socket
import subprocess
import sys
import time
import traceback
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import yaml
from impala.dbapi import connect


APP_NAME = "ai_lake_preprocessing"
LOGGER = logging.getLogger(APP_NAME)
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DATETIME_TOKEN_RE = re.compile(r"^[A-Za-z0-9 _:/.,+'\-]+$")


class ConfigError(ValueError):
    """설정 파일이 잘못되었을 때 사용하는 예외."""


class ValidationError(RuntimeError):
    """데이터를 변경하기 전 검증이 실패했을 때 사용하는 예외."""


class KerberosError(RuntimeError):
    """Kerberos 설정, keytab 또는 ticket 검증이 실패한 경우의 예외."""


class DailyFileHandler(logging.Handler):
    """Asia/Seoul 기준 날짜별 로그 파일에 메시지를 기록한다.

    일반 TimedRotatingFileHandler는 현재 파일명이 고정되는 경우가 있어,
    요구사항대로 항상 preprocessing_YYYYMMDD.log 이름을 사용하도록 작게
    구현했다. 배치는 파일 잠금으로 한 번에 하나만 실행되므로 동시 쓰기를
    별도로 처리하지 않는다.
    """

    def __init__(self, log_dir: Path) -> None:
        super().__init__()
        self.log_dir = log_dir
        self.current_date: Optional[str] = None
        self.stream = None

    def emit(self, record: logging.LogRecord) -> None:
        try:
            date_text = datetime.now().strftime("%Y%m%d")
            if date_text != self.current_date:
                self._open_stream(date_text)
            assert self.stream is not None
            self.stream.write(self.format(record) + "\n")
            self.flush()
        except Exception:
            self.handleError(record)

    def _open_stream(self, date_text: str) -> None:
        if self.stream:
            self.stream.close()
        path = self.log_dir / ("preprocessing_%s.log" % date_text)
        self.stream = path.open("a", encoding="utf-8")
        self.current_date = date_text

    def flush(self) -> None:
        if self.stream:
            self.stream.flush()

    def close(self) -> None:
        if self.stream:
            self.stream.close()
            self.stream = None
        super().close()


class ProcessLock:
    """수동 실행과 cron 실행이 겹치지 않도록 프로세스 잠금을 잡는다."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.handle = None

    def __enter__(self) -> "ProcessLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+")
        try:
            if os.name == "nt":
                import msvcrt

                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, IOError) as exc:
            self.handle.close()
            self.handle = None
            raise ValidationError("이미 다른 전처리 배치가 실행 중입니다.") from exc

        self.handle.seek(0)
        self.handle.truncate()
        self.handle.write(str(os.getpid()))
        self.handle.flush()
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, exc_tb: Any) -> None:
        if not self.handle:
            return
        try:
            if os.name == "nt":
                import msvcrt

                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        finally:
            self.handle.close()
            self.handle = None


def parse_args() -> argparse.Namespace:
    """명령행 옵션을 정의한다."""
    parser = argparse.ArgumentParser(description="ai_lake Kudu 전처리 배치")
    parser.add_argument("--config", default="config.yaml", help="YAML 설정 파일")
    parser.add_argument("--table", help="설정의 name과 일치하는 테이블 하나만 실행")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="검증과 예상 건수 조회만 하고 대상 데이터는 변경하지 않음",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="설정과 테이블 구조만 확인하고 건수 조회 및 적재는 하지 않음",
    )
    parser.add_argument("--debug", action="store_true", help="DEBUG 로그 활성화")
    return parser.parse_args()


def setup_logging(log_dir: Path, debug: bool) -> None:
    """콘솔과 날짜별 파일에 동일한 형식으로 로그를 기록한다."""
    log_dir.mkdir(parents=True, exist_ok=True)
    test_path = log_dir / ".write_test"
    try:
        test_path.write_text("ok", encoding="utf-8")
        test_path.unlink()
    except OSError as exc:
        raise RuntimeError("로그 디렉터리에 쓸 수 없습니다: %s" % log_dir) from exc

    level = logging.DEBUG if debug else logging.INFO
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s", "%Y-%m-%d %H:%M:%S"
    )

    LOGGER.setLevel(level)
    LOGGER.handlers.clear()

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(formatter)
    LOGGER.addHandler(console)

    daily_file = DailyFileHandler(log_dir)
    daily_file.setLevel(level)
    daily_file.setFormatter(formatter)
    LOGGER.addHandler(daily_file)


def load_config(path: Path) -> Dict[str, Any]:
    """YAML 설정을 읽고 최상위 필수 항목을 검사한다."""
    if not path.exists():
        raise ConfigError("설정 파일이 없습니다: %s" % path)
    with path.open("r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle) or {}

    if not isinstance(config.get("impala"), dict):
        raise ConfigError("impala 설정이 필요합니다.")
    if not isinstance(config.get("tables"), list) or not config["tables"]:
        raise ConfigError("tables 설정에 처리할 테이블이 필요합니다.")

    history = config.setdefault("history", {})
    history.setdefault("table", "ai_datawarehouse.preprocessing_history")
    history.setdefault("change_table", "ai_datawarehouse.preprocessing_change_history")
    runtime = config.setdefault("runtime", {})
    runtime.setdefault("log_dir", "logs")
    runtime.setdefault("lock_file", "run/preprocessing.lock")
    kerberos = config.setdefault("kerberos", {}) or {}
    config["kerberos"] = kerberos
    kerberos.setdefault("enabled", False)
    kerberos.setdefault("manage_ticket", True)
    kerberos.setdefault("cache_dir", "run/krb5_cache")
    features = config.setdefault("features", {}) or {}
    config["features"] = features
    features.setdefault("domain_rules_enabled", False)
    return config


def validate_identifier(value: str, label: str) -> str:
    """SQL 식별자에 허용되지 않은 문자가 들어가는 것을 막는다."""
    if not isinstance(value, str) or not IDENTIFIER_RE.fullmatch(value):
        raise ConfigError("%s에 올바르지 않은 식별자가 있습니다: %r" % (label, value))
    return value


def quote_identifier(value: str, label: str = "identifier") -> str:
    return "`%s`" % validate_identifier(value, label)


def quote_table(value: str, label: str = "table") -> str:
    parts = value.split(".")
    if len(parts) != 2:
        raise ConfigError("%s은 schema.table 형식이어야 합니다: %r" % (label, value))
    return ".".join(quote_identifier(part, label) for part in parts)


def sql_literal(value: Any) -> str:
    """설정값을 안전한 SQL 리터럴로 변환한다."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return "'%s'" % str(value).replace("'", "''")


def validate_datetime_format(value: str, label: str) -> str:
    """Impala 날짜 포맷 문자열에 SQL 제어문자가 들어가는 것을 막는다."""
    if not isinstance(value, str) or not value or not DATETIME_TOKEN_RE.fullmatch(value):
        raise ConfigError("%s이 올바르지 않습니다: %r" % (label, value))
    return value


def validate_table_config(table: Dict[str, Any]) -> None:
    """테이블 하나의 설정 형식과 필수값을 검사한다."""
    for field in ("name", "source", "target"):
        if not table.get(field):
            raise ConfigError("테이블 설정에 %s 값이 필요합니다." % field)
    validate_identifier(table["name"], "table.name")
    quote_table(table["source"], "source")
    quote_table(table["target"], "target")

    load = table.get("load", {}) or {}
    mode = load.get("mode", "full")
    if mode not in ("full", "date_range"):
        raise ConfigError("load.mode은 full 또는 date_range만 가능합니다.")
    if mode == "date_range":
        validate_identifier(load.get("date_column"), "load.date_column")
        for field in ("start_date", "end_date"):
            value = str(load.get(field, ""))
            try:
                datetime.strptime(value, "%Y-%m-%d")
            except ValueError as exc:
                raise ConfigError("load.%s은 YYYY-MM-DD 형식이어야 합니다." % field) from exc
        if str(load["start_date"]) >= str(load["end_date"]):
            raise ConfigError("load.end_date는 start_date보다 뒤여야 합니다.")
        interval_days = load.get("interval_days", 1)
        if not isinstance(interval_days, int) or isinstance(interval_days, bool) or interval_days < 1:
            raise ConfigError("load.interval_days는 1 이상의 정수여야 합니다.")
        supported_formats = (
            "yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss", "yyyyMMdd", "yyyyMMddHHmmss"
        )
        for field in ("source_format", "target_format"):
            value = load.get(field, "yyyy-MM-dd HH:mm:ss")
            if value not in supported_formats:
                raise ConfigError(
                    "load.%s은 지원 형식(%s) 중 하나여야 합니다."
                    % (field, ", ".join(supported_formats))
                )

    trim = table.get("trim", {}) or {}
    for column in trim.get("exclude_columns", []):
        validate_identifier(column, "trim.exclude_columns")
    trim_columns = trim.get("columns")
    if trim_columns is not None:
        if not isinstance(trim_columns, list):
            raise ConfigError("trim.columns는 목록이어야 합니다.")
        names = set()
        for index, rule in enumerate(trim_columns, start=1):
            if not isinstance(rule, dict):
                raise ConfigError("trim.columns[%d]는 객체여야 합니다." % index)
            column = rule.get("column")
            validate_identifier(column, "trim.columns[%d].column" % index)
            if column in names:
                raise ConfigError("trim.columns에 중복된 컬럼이 있습니다: %s" % column)
            names.add(column)
            if column in trim.get("exclude_columns", []):
                raise ConfigError("trim.columns와 exclude_columns에 동시에 지정할 수 없습니다: %s" % column)
            for option in ("enabled", "trim", "collapse_spaces", "collapse_newlines", "newline_to_space"):
                if option in rule and not isinstance(rule[option], bool):
                    raise ConfigError("trim.columns.%s는 true 또는 false여야 합니다." % option)
            if rule.get("collapse_newlines") and rule.get("newline_to_space"):
                raise ConfigError(
                    "collapse_newlines와 newline_to_space는 동시에 true일 수 없습니다: %s" % column
                )

    for rule in table.get("datetime", []) or []:
        column = rule.get("column")
        validate_identifier(column, "datetime.column")
        formats = rule.get("input_formats") or ([rule["input_format"]] if rule.get("input_format") else [])
        # TIMESTAMP 원천은 이미 날짜 타입이므로 입력 포맷이 필요하지 않다.
        # STRING 원천에 대한 입력 포맷 필수 검사는 실제 스키마를 읽은 뒤 수행한다.
        for item in formats:
            validate_datetime_format(item, "datetime.input_format")
        validate_datetime_format(
            rule.get("output_format", "yyyy-MM-dd HH:mm:ss"), "datetime.output_format"
        )
        if rule.get("invalid_action", "null") not in ("null", "fail"):
            raise ConfigError("datetime.invalid_action은 null 또는 fail만 가능합니다.")

    deduplicate = table.get("deduplicate", {}) or {}
    if deduplicate.get("enabled"):
        keys = deduplicate.get("keys") or []
        if not keys and not deduplicate.get("allow_target_primary_key_fallback", False):
            raise ConfigError(
                "중복 제거에는 업무 중복 기준인 deduplicate.keys가 필요합니다. "
                "대상 Kudu PK를 의도적으로 사용하려면 "
                "allow_target_primary_key_fallback: true를 설정하십시오."
            )
        for column in keys:
            validate_identifier(column, "deduplicate.keys")
        for order in deduplicate.get("order_by", []):
            validate_identifier(order.get("column"), "deduplicate.order_by.column")
            if str(order.get("direction", "desc")).lower() not in ("asc", "desc"):
                raise ConfigError("중복 제거 정렬 방향은 asc 또는 desc만 가능합니다.")
    noise = table.get("noise_filters", {}) or {}
    if noise.get("enabled"):
        rules = noise.get("rules") or []
        if not rules:
            raise ConfigError("noise_filters.enabled=true이면 rules가 필요합니다.")
        names = set()
        supported_operators = (
            "equals", "not_equals", "contains", "starts_with", "ends_with", "regex", "in"
        )
        for index, rule in enumerate(rules, start=1):
            name = rule.get("name")
            validate_identifier(name, "noise_filters.rules[%d].name" % index)
            if name in names:
                raise ConfigError("중복된 노이즈 규칙 name입니다: %s" % name)
            names.add(name)
            validate_identifier(rule.get("column"), "noise_filters.rules[%d].column" % index)
            operator = rule.get("operator")
            if operator not in supported_operators:
                raise ConfigError(
                    "noise_filters operator는 %s 중 하나여야 합니다." % ", ".join(supported_operators)
                )
            if operator == "in":
                if not isinstance(rule.get("values"), list) or not rule["values"]:
                    raise ConfigError("noise_filters의 in 연산자는 values 목록이 필요합니다.")
            elif rule.get("value") is None:
                raise ConfigError("noise_filters 규칙에 value가 필요합니다: %s" % name)
            if rule.get("action", "exclude") != "exclude":
                raise ConfigError("현재 noise_filters.action은 exclude만 지원합니다.")

    domain = table.get("domain_rules", {}) or {}
    if domain.get("enabled"):
        rules = domain.get("rules") or []
        if not rules:
            raise ConfigError("domain_rules.enabled=true이면 rules가 필요합니다.")
        names = set()
        supported = {
            "fill": {"is_blank"},
            "exclude": {"not_in", "less_than", "greater_than_current_datetime"},
        }
        for index, rule in enumerate(rules, start=1):
            name = rule.get("name")
            validate_identifier(name, "domain_rules.rules[%d].name" % index)
            if name in names:
                raise ConfigError("중복된 도메인 규칙 name입니다: %s" % name)
            names.add(name)
            validate_identifier(rule.get("column"), "domain_rules.rules[%d].column" % index)
            action = rule.get("action")
            operator = rule.get("operator")
            if action not in supported or operator not in supported[action]:
                raise ConfigError("지원하지 않는 도메인 규칙입니다: %s/%s" % (action, operator))
            if operator == "not_in":
                if not isinstance(rule.get("values"), list) or not rule["values"]:
                    raise ConfigError("domain_rules의 not_in에는 values 목록이 필요합니다.")
            elif operator != "greater_than_current_datetime" and rule.get("value") is None:
                raise ConfigError("domain_rules 규칙에 value가 필요합니다: %s" % name)


def deduplication_keys(
    deduplicate: Dict[str, Any], target_primary_keys: Sequence[str]
) -> List[str]:
    """명시한 업무 키를 우선 사용하고, 허용된 경우에만 Kudu PK를 사용한다."""
    keys = list(deduplicate.get("keys") or [])
    if keys:
        return keys
    if deduplicate.get("allow_target_primary_key_fallback", False):
        if not target_primary_keys:
            raise ValidationError("대상 Kudu PRIMARY KEY를 조회할 수 없습니다.")
        return list(target_primary_keys)
    raise ValidationError("중복 제거 업무 키(deduplicate.keys)가 없습니다.")


def connect_impala(settings: Dict[str, Any]):
    """현재 Kerberos ticket을 사용하여 Impala에 연결한다."""
    required = ("host", "port")
    for key in required:
        if settings.get(key) in (None, ""):
            raise ConfigError("impala.%s 설정이 필요합니다." % key)

    kwargs: Dict[str, Any] = {
        "host": settings["host"],
        "port": int(settings["port"]),
        "auth_mechanism": settings.get("auth_mechanism", "GSSAPI"),
        "kerberos_service_name": settings.get("kerberos_service_name", "impala"),
        "use_ssl": bool(settings.get("use_ssl", False)),
        "timeout": int(settings.get("timeout", 60)),
    }
    if settings.get("ca_cert"):
        kwargs["ca_cert"] = settings["ca_cert"]
    if settings.get("use_http_transport"):
        kwargs["use_http_transport"] = True
        kwargs["http_path"] = settings.get("http_path", "")

    return connect(**kwargs)


def kerberos_command(name: str) -> str:
    """운영 서버에 필요한 Kerberos 명령이 설치되어 있는지 확인한다."""
    path = shutil.which(name)
    if not path:
        raise KerberosError("Kerberos 명령을 찾을 수 없습니다: %s" % name)
    return path


def run_kerberos_command(command: Sequence[str], environment: Dict[str, str]) -> subprocess.CompletedProcess:
    """민감한 명령 인자를 로그에 남기지 않고 Kerberos 명령을 실행한다."""
    try:
        return subprocess.run(
            list(command),
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            check=False,
        )
    except OSError as exc:
        raise KerberosError("Kerberos 명령 실행에 실패했습니다.") from exc


def resolve_config_path(value: str, project_dir: Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else project_dir / path


def prepare_kerberos(config: Dict[str, Any], project_dir: Path) -> Optional[Dict[str, Any]]:
    """YAML과 환경변수로 Kerberos ticket을 준비하고 실행 상태를 반환한다.

    keytab과 principal은 환경변수 BATCH_KEYTAB/BATCH_KRB_PRINCIPAL이 있으면
    YAML보다 우선한다. 프로세스마다 고유한 credential cache를 사용한다.
    """
    settings = config.get("kerberos", {}) or {}
    if not settings.get("enabled", False):
        LOGGER.info("kerberos_management=DISABLED using_existing_process_environment=true")
        return None
    if os.name == "nt":
        raise KerberosError("내장 Kerberos 인증 관리는 Linux collect 서버에서만 지원합니다.")

    klist = kerberos_command("klist")
    environment = dict(os.environ)
    previous_cache = os.environ.get("KRB5CCNAME")
    previous_config = os.environ.get("KRB5_CONFIG")

    krb5_config_value = os.environ.get("KRB5_CONFIG") or settings.get("krb5_config")
    if krb5_config_value:
        krb5_config = resolve_config_path(str(krb5_config_value), project_dir)
        if not krb5_config.is_file():
            raise KerberosError("krb5.conf 파일이 없습니다: %s" % krb5_config)
        environment["KRB5_CONFIG"] = str(krb5_config)

    manage_ticket = bool(settings.get("manage_ticket", True))
    state: Dict[str, Any] = {
        "managed": manage_ticket,
        "environment": environment,
        "previous_cache": previous_cache,
        "previous_config": previous_config,
        "klist": klist,
    }

    if not manage_ticket:
        check = run_kerberos_command([klist, "-s"], environment)
        if check.returncode != 0:
            raise KerberosError("유효한 기존 Kerberos ticket이 없습니다.")
        if environment.get("KRB5_CONFIG"):
            os.environ["KRB5_CONFIG"] = environment["KRB5_CONFIG"]
        LOGGER.info("kerberos_ticket_status=VALID managed=false")
        return state

    principal = os.environ.get("BATCH_KRB_PRINCIPAL") or settings.get("principal")
    keytab_value = os.environ.get("BATCH_KEYTAB") or settings.get("keytab")
    if not principal:
        raise KerberosError("kerberos.principal 또는 BATCH_KRB_PRINCIPAL이 필요합니다.")
    if not keytab_value:
        raise KerberosError("kerberos.keytab 또는 BATCH_KEYTAB이 필요합니다.")

    keytab = Path(str(keytab_value))
    if not keytab.is_absolute():
        raise KerberosError("kerberos.keytab은 서버의 절대 경로여야 합니다.")
    if not keytab.is_file():
        raise KerberosError("keytab 파일이 없습니다: %s" % keytab)
    if not os.access(str(keytab), os.R_OK):
        raise KerberosError("keytab 파일을 읽을 권한이 없습니다.")
    try:
        if keytab.stat().st_mode & 0o077:
            LOGGER.warning("kerberos_keytab_permissions_are_broad=true recommended_mode=600")
    except OSError as exc:
        raise KerberosError("keytab 파일 정보를 확인할 수 없습니다.") from exc

    kinit = kerberos_command("kinit")
    kdestroy = kerberos_command("kdestroy")
    keytab_check = run_kerberos_command([klist, "-k", str(keytab)], environment)
    if keytab_check.returncode != 0:
        raise KerberosError("keytab 파일을 확인할 수 없습니다.")
    if str(principal) not in keytab_check.stdout:
        raise KerberosError("keytab에 설정한 principal이 없습니다: %s" % principal)

    cache_dir = resolve_config_path(str(settings.get("cache_dir", "run/krb5_cache")), project_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    try:
        cache_dir.chmod(0o700)
    except OSError:
        LOGGER.warning("kerberos_cache_dir_chmod_failed=true path=%s", cache_dir)
    cache_path = cache_dir / ("krb5cc_%s_%s" % (os.getpid(), uuid.uuid4().hex))
    cache_name = "FILE:%s" % cache_path
    environment["KRB5CCNAME"] = cache_name

    state.update(
        {
            "principal": str(principal),
            "keytab": str(keytab),
            "cache_path": cache_path,
            "cache_name": cache_name,
            "kinit": kinit,
            "kdestroy": kdestroy,
        }
    )
    refresh_kerberos_ticket(state)
    if environment.get("KRB5_CONFIG"):
        os.environ["KRB5_CONFIG"] = environment["KRB5_CONFIG"]
    os.environ["KRB5CCNAME"] = cache_name
    LOGGER.info("kerberos_ticket_status=VALID managed=true principal=%s", principal)
    return state


def refresh_kerberos_ticket(state: Dict[str, Any]) -> None:
    """keytab으로 ticket을 다시 발급하고 principal까지 확인한다."""
    environment = state["environment"]
    result = run_kerberos_command(
        [state["kinit"], "-kt", state["keytab"], state["principal"]], environment
    )
    if result.returncode != 0:
        raise KerberosError("keytab으로 Kerberos ticket 발급에 실패했습니다.")
    check = run_kerberos_command([state["klist"], "-s"], environment)
    if check.returncode != 0:
        raise KerberosError("발급된 Kerberos ticket이 유효하지 않습니다.")
    detail = run_kerberos_command([state["klist"], "-c", state["cache_name"]], environment)
    if detail.returncode != 0 or state["principal"] not in detail.stdout:
        raise KerberosError("발급된 ticket principal이 설정과 일치하지 않습니다.")


def ensure_kerberos_ticket(state: Optional[Dict[str, Any]]) -> None:
    """테이블 처리 전에 ticket을 확인하고, 관리 모드면 만료 시 재발급한다."""
    if not state:
        return
    check = run_kerberos_command([state["klist"], "-s"], state["environment"])
    if check.returncode == 0:
        return
    if not state.get("managed"):
        raise KerberosError("기존 Kerberos ticket이 만료되었습니다.")
    LOGGER.warning("kerberos_ticket_status=EXPIRED action=REINITIALIZE")
    refresh_kerberos_ticket(state)


def cleanup_kerberos(state: Optional[Dict[str, Any]]) -> None:
    """이 프로세스가 만든 ticket cache만 제거하고 기존 환경을 복원한다."""
    if not state:
        return
    if state.get("managed"):
        run_kerberos_command(
            [state["kdestroy"], "-c", state["cache_name"]], state["environment"]
        )
        cache_path = state.get("cache_path")
        if cache_path and Path(cache_path).exists():
            try:
                Path(cache_path).unlink()
            except OSError:
                LOGGER.warning("kerberos_cache_cleanup_failed=true")
    if state.get("previous_cache") is None:
        os.environ.pop("KRB5CCNAME", None)
    else:
        os.environ["KRB5CCNAME"] = state["previous_cache"]
    if state.get("previous_config") is None:
        os.environ.pop("KRB5_CONFIG", None)
    else:
        os.environ["KRB5_CONFIG"] = state["previous_config"]


def execute(cursor: Any, sql: str, fetch: bool = False) -> List[Tuple[Any, ...]]:
    """SQL을 실행하고 DEBUG 모드에서만 전체 SQL을 기록한다."""
    LOGGER.debug("sql=%s", " ".join(sql.split()))
    cursor.execute(sql)
    return list(cursor.fetchall()) if fetch else []


def scalar(cursor: Any, sql: str) -> int:
    rows = execute(cursor, sql, fetch=True)
    return int(rows[0][0]) if rows else 0


def get_query_id(cursor: Any) -> Optional[str]:
    """impyla 버전에 따라 가능한 경우에만 Impala query ID를 가져온다."""
    operation = getattr(cursor, "_last_operation", None)
    handle = getattr(operation, "handle", None)
    operation_id = getattr(handle, "operationId", None)
    guid = getattr(operation_id, "guid", None)
    if isinstance(guid, bytes):
        return guid.hex()
    return str(guid) if guid else None


def describe_table(cursor: Any, table_name: str) -> List[Dict[str, str]]:
    """DESCRIBE 결과에서 실제 컬럼명과 타입만 추출한다."""
    rows = execute(cursor, "DESCRIBE %s" % quote_table(table_name), fetch=True)
    columns: List[Dict[str, str]] = []
    for row in rows:
        name = str(row[0]).strip() if row and row[0] is not None else ""
        data_type = str(row[1]).strip().lower() if len(row) > 1 and row[1] else ""
        if not name or name.startswith("#") or not IDENTIFIER_RE.fullmatch(name):
            continue
        columns.append({"name": name, "type": data_type})
    if not columns:
        raise ValidationError("테이블 컬럼을 조회할 수 없습니다: %s" % table_name)
    return columns


def get_primary_keys(cursor: Any, table_name: str) -> List[str]:
    """SHOW CREATE TABLE에서 Kudu PRIMARY KEY 컬럼을 읽는다."""
    rows = execute(cursor, "SHOW CREATE TABLE %s" % quote_table(table_name), fetch=True)
    ddl = "\n".join(str(row[0]) for row in rows if row)
    match = re.search(r"PRIMARY\s+KEY\s*\(([^)]+)\)", ddl, re.IGNORECASE)
    if not match:
        # 단일 컬럼의 inline PRIMARY KEY 표기도 지원한다.
        inline = re.search(r"[`]?([A-Za-z_][A-Za-z0-9_]*)[`]?\s+[^,\n]+PRIMARY\s+KEY", ddl, re.IGNORECASE)
        return [inline.group(1)] if inline else []
    return [part.strip().strip("`") for part in match.group(1).split(",")]


def column_map(columns: Sequence[Dict[str, str]]) -> Dict[str, str]:
    return {column["name"]: column["type"] for column in columns}


def is_string_type(data_type: str) -> bool:
    """Impala의 문자열 계열 타입인지 반환한다."""
    return data_type.startswith(("string", "varchar", "char"))


def datetime_expression(
    column: str, rule: Dict[str, Any], source_type: str = "string"
) -> str:
    """STRING 또는 TIMESTAMP 날짜를 DW용 표준 문자열로 변환한다."""
    output_format = rule.get("output_format", "yyyy-MM-dd HH:mm:ss")
    quoted = quote_identifier(column)

    # TIMESTAMP는 문자열 파싱이 필요 없다. FROM_TIMESTAMP로 표시 형식만
    # 바꾸며, yyyy-MM-dd'T'HH:mm:ss처럼 T 구분자도 사용할 수 있다.
    if source_type.startswith("timestamp"):
        return "FROM_TIMESTAMP(%s, %s)" % (quoted, sql_literal(output_format))

    input_formats = rule.get("input_formats") or (
        [rule["input_format"]] if rule.get("input_format") else []
    )
    if not input_formats:
        raise ValidationError("STRING 날짜 컬럼에는 input_format(s)이 필요합니다: %s" % column)
    source = "TRIM(%s)" % quoted
    candidates = [
        "FROM_UNIXTIME(UNIX_TIMESTAMP(%s, %s), %s)"
        % (source, sql_literal(item), sql_literal(output_format))
        for item in input_formats
    ]
    return "COALESCE(%s)" % ", ".join(candidates)


def trim_column_rules(
    table: Dict[str, Any], source_types: Dict[str, str]
) -> Dict[str, Dict[str, bool]]:
    """테이블 설정을 컬럼별 문자열 정리 옵션으로 변환한다.

    columns가 없으면 기존 호환 방식으로 모든 문자열 컬럼에 양끝 TRIM만
    적용한다. columns가 있으면 목록에 적힌 활성 컬럼만 처리한다.
    """
    trim = table.get("trim", {}) or {}
    if not trim.get("enabled", True):
        return {}
    excluded = set(trim.get("exclude_columns", []))
    configured = trim.get("columns")
    rules: Dict[str, Dict[str, bool]] = {}

    if configured is None:
        for column, data_type in source_types.items():
            if data_type.startswith(("string", "varchar", "char")) and column not in excluded:
                rules[column] = {
                    "trim": True,
                    "collapse_spaces": False,
                    "collapse_newlines": False,
                    "newline_to_space": False,
                }
        return rules

    for rule in configured:
        column = rule["column"]
        if not rule.get("enabled", True):
            continue
        if column not in source_types:
            raise ValidationError("공백 전처리 컬럼이 원천에 없습니다: %s" % column)
        if not source_types[column].startswith(("string", "varchar", "char")):
            raise ValidationError("공백 전처리는 문자열 컬럼만 지원합니다: %s" % column)
        rules[column] = {
            "trim": rule.get("trim", True),
            "collapse_spaces": rule.get("collapse_spaces", False),
            "collapse_newlines": rule.get("collapse_newlines", False),
            "newline_to_space": rule.get("newline_to_space", False),
        }
    return rules


def text_cleanup_expression(expression: str, options: Dict[str, bool]) -> str:
    """선택한 공백/개행 옵션을 순서대로 적용한 Impala SQL 식을 만든다."""
    result = expression
    if options.get("newline_to_space"):
        # CR과 LF를 각각 일반 공백으로 바꾼다. collapse_spaces도 켜져 있으면
        # 개행 치환으로 생긴 연속 공백까지 한 칸으로 합쳐진다.
        result = "REPLACE(REPLACE(%s, CHR(13), ' '), CHR(10), ' ')" % result
    elif options.get("collapse_newlines"):
        # CRLF와 단독 CR을 LF로 통일한 뒤 연속 LF를 하나로 줄인다.
        result = "REPLACE(%s, CONCAT(CHR(13), CHR(10)), CHR(10))" % result
        result = "REPLACE(%s, CHR(13), CHR(10))" % result
        result = "REGEXP_REPLACE(%s, CONCAT(CHR(10), '+'), CHR(10))" % result
    if options.get("collapse_spaces"):
        result = "REGEXP_REPLACE(%s, ' +', ' ')" % result
    if options.get("trim", True):
        result = "TRIM(%s)" % result
    return result


def format_range_boundary(value: datetime, value_format: str) -> str:
    """날짜 구간 경계를 설정된 원천/대상 문자열 형식으로 만든다."""
    formats = {
        "yyyy-MM-dd": "%Y-%m-%d",
        "yyyy-MM-dd HH:mm:ss": "%Y-%m-%d %H:%M:%S",
        "yyyyMMdd": "%Y%m%d",
        "yyyyMMddHHmmss": "%Y%m%d%H%M%S",
    }
    return value.strftime(formats[value_format])


def build_date_ranges(load: Dict[str, Any]) -> List[Tuple[datetime, datetime]]:
    """[시작일, 종료일) 범위를 interval_days 단위의 반개구간으로 나눈다."""
    if load.get("mode", "full") != "date_range":
        return []
    current = datetime.strptime(str(load["start_date"]), "%Y-%m-%d")
    end = datetime.strptime(str(load["end_date"]), "%Y-%m-%d")
    step = timedelta(days=int(load.get("interval_days", 1)))
    ranges: List[Tuple[datetime, datetime]] = []
    while current < end:
        next_value = min(current + step, end)
        ranges.append((current, next_value))
        current = next_value
    return ranges


def build_range_condition(
    date_column: str,
    start: datetime,
    end: datetime,
    value_format: str,
    data_type: str = "string",
) -> str:
    """문자열/DATE/TIMESTAMP 모두 비교 가능한 날짜 범위 조건을 만든다.

    STRING 컬럼은 값이 시간순으로 정렬되는 지원 형식이어야 한다. DATE와
    TIMESTAMP 컬럼은 Impala가 문자열 리터럴을 해당 타입으로 비교한다.
    """
    column = quote_identifier(date_column, "load.date_column")
    start_value = format_range_boundary(start, value_format)
    end_value = format_range_boundary(end, value_format)
    start_literal = sql_literal(start_value)
    end_literal = sql_literal(end_value)
    if data_type.startswith("date"):
        start_literal = "CAST(%s AS DATE)" % start_literal
        end_literal = "CAST(%s AS DATE)" % end_literal
    elif data_type.startswith("timestamp"):
        start_literal = "CAST(%s AS TIMESTAMP)" % start_literal
        end_literal = "CAST(%s AS TIMESTAMP)" % end_literal
    return "%s >= %s AND %s < %s" % (
        column, start_literal, column, end_literal
    )


def noise_rule_expression(rule: Dict[str, Any], source_types: Dict[str, str]) -> str:
    """검증된 YAML 노이즈 규칙 하나를 안전한 Impala 조건식으로 만든다."""
    column_name = rule["column"]
    if column_name not in source_types:
        raise ValidationError("노이즈 필터 컬럼이 원천에 없습니다: %s" % column_name)
    column = "CAST(%s AS STRING)" % quote_identifier(column_name)
    case_sensitive = bool(rule.get("case_sensitive", False))
    comparable = column if case_sensitive else "LOWER(%s)" % column

    def normalized_literal(value: Any) -> str:
        text = str(value) if case_sensitive else str(value).lower()
        return sql_literal(text)

    operator = rule["operator"]
    if operator == "equals":
        return "%s = %s" % (comparable, normalized_literal(rule["value"]))
    if operator == "not_equals":
        return "%s <> %s" % (comparable, normalized_literal(rule["value"]))
    if operator == "contains":
        return "INSTR(%s, %s) > 0" % (comparable, normalized_literal(rule["value"]))
    if operator == "starts_with":
        value = normalized_literal(rule["value"])
        return "SUBSTR(%s, 1, LENGTH(%s)) = %s" % (comparable, value, value)
    if operator == "ends_with":
        value = normalized_literal(rule["value"])
        return "RIGHT(%s, LENGTH(%s)) = %s" % (comparable, value, value)
    if operator == "regex":
        return "%s RLIKE %s" % (comparable, normalized_literal(rule["value"]))
    values = ", ".join(normalized_literal(value) for value in rule["values"])
    return "%s IN (%s)" % (comparable, values)


def build_noise_condition(table: Dict[str, Any], source_types: Dict[str, str]) -> Optional[str]:
    """활성 노이즈 규칙 중 하나라도 일치하는 행의 조건을 반환한다."""
    noise = table.get("noise_filters", {}) or {}
    if not noise.get("enabled"):
        return None
    expressions = [noise_rule_expression(rule, source_types) for rule in noise.get("rules", [])]
    return " OR ".join("(%s)" % expression for expression in expressions)


def combine_conditions(*conditions: Optional[str]) -> Optional[str]:
    """비어 있지 않은 조건들을 AND로 결합한다."""
    values = ["(%s)" % condition for condition in conditions if condition]
    return " AND ".join(values) if values else None


def domain_rules_active(table: Dict[str, Any]) -> bool:
    """전역 기능 스위치와 테이블 스위치가 모두 켜졌는지 확인한다."""
    return bool(
        table.get("_domain_rules_execution_enabled", False)
        and (table.get("domain_rules", {}) or {}).get("enabled", False)
    )


def domain_condition_expression(rule: Dict[str, Any], expression: str) -> str:
    """도메인 규칙 조건을 Impala SQL로 변환한다."""
    operator = rule["operator"]
    if operator == "is_blank":
        return "(%s IS NULL OR TRIM(CAST(%s AS STRING)) = '')" % (expression, expression)
    if operator == "not_in":
        values = ", ".join(sql_literal(value) for value in rule["values"])
        return "(%s IS NULL OR CAST(%s AS STRING) NOT IN (%s))" % (expression, expression, values)
    if operator == "less_than":
        return "CAST(%s AS DOUBLE) < %s" % (expression, sql_literal(rule["value"]))
    if operator == "greater_than_current_datetime":
        return "CAST(%s AS TIMESTAMP) > NOW()" % expression
    raise ConfigError("지원하지 않는 domain operator입니다: %s" % operator)


def build_domain_exclusion_condition(
    table: Dict[str, Any],
    source_types: Dict[str, str],
    column_expressions: Dict[str, str],
) -> Optional[str]:
    """활성화된 도메인 제외 규칙 중 하나라도 일치하는 조건을 만든다."""
    if not domain_rules_active(table):
        return None
    expressions: List[str] = []
    for rule in (table.get("domain_rules", {}) or {}).get("rules", []):
        column = rule["column"]
        if column not in source_types:
            raise ValidationError("도메인 규칙 컬럼이 원천에 없습니다: %s" % column)
        if rule["action"] == "exclude":
            expressions.append(domain_condition_expression(rule, column_expressions[column]))
    return " OR ".join("(%s)" % value for value in expressions) if expressions else None


def build_transform_sql(
    table: Dict[str, Any],
    source_columns: Sequence[Dict[str, str]],
    target_columns: Sequence[Dict[str, str]],
    target_primary_keys: Sequence[str],
    source_condition: Optional[str] = None,
) -> Tuple[str, List[str], List[str], Dict[str, Any]]:
    """공백·날짜·중복 규칙을 반영한 최종 SELECT SQL을 만든다."""
    source_types = column_map(source_columns)
    target_types = column_map(target_columns)
    target_order = [column["name"] for column in target_columns]

    missing = [name for name in target_order if name not in source_types]
    if missing:
        raise ValidationError("원천에 없는 대상 컬럼: %s" % ", ".join(missing))

    # 날짜 변환 컬럼을 제외한 일반 컬럼은 원천과 대상 타입이 같아야 한다.
    # 암묵적 형변환에 기대면 값 손실이나 실행 시점 오류가 생길 수 있다.
    datetime_column_names = {
        rule["column"] for rule in table.get("datetime", []) or []
    }
    incompatible = [
        "%s(%s->%s)" % (name, source_types[name], target_types[name])
        for name in target_order
        if name not in datetime_column_names and source_types[name] != target_types[name]
    ]
    if incompatible:
        raise ValidationError("원천과 대상 컬럼 타입이 다릅니다: %s" % ", ".join(incompatible))

    trim = table.get("trim", {}) or {}
    trim_enabled = trim.get("enabled", True)
    excluded = set(trim.get("exclude_columns", []))
    datetime_rules = {rule["column"]: rule for rule in table.get("datetime", []) or []}
    cleanup_rules = trim_column_rules(table, source_types)

    for column in excluded:
        if column not in source_types:
            raise ValidationError("공백 제거 제외 컬럼이 원천에 없습니다: %s" % column)
    for column in datetime_rules:
        if column not in source_types:
            raise ValidationError("날짜 변환 컬럼이 원천에 없습니다: %s" % column)
        if not is_string_type(target_types[column]):
            raise ValidationError("날짜 변환 대상 컬럼은 STRING 계열이어야 합니다: %s" % column)
        if not (is_string_type(source_types[column]) or source_types[column].startswith("timestamp")):
            raise ValidationError(
                "날짜 변환 원천 컬럼은 STRING 계열 또는 TIMESTAMP여야 합니다: %s" % column
            )
        if is_string_type(source_types[column]):
            rule = datetime_rules[column]
            if not (rule.get("input_formats") or rule.get("input_format")):
                raise ValidationError(
                    "STRING 날짜 컬럼에는 input_format(s)이 필요합니다: %s" % column
                )

    expressions: List[str] = []
    column_expressions: Dict[str, str] = {}
    trimmed_columns: List[str] = []
    for column in target_order:
        quoted = quote_identifier(column)
        if column in datetime_rules:
            expression = datetime_expression(
                column, datetime_rules[column], source_types[column]
            )
        elif column in cleanup_rules:
            expression = text_cleanup_expression(quoted, cleanup_rules[column])
            trimmed_columns.append(column)
        else:
            expression = quoted
        column_expressions[column] = expression

    # 기능 스위치가 꺼져 있으면 이 블록은 규칙 컬럼을 확인하거나 SQL에
    # 영향을 주지 않는다. 활성화된 경우에만 fill 규칙을 CASE 식으로 적용한다.
    if domain_rules_active(table):
        for rule in (table.get("domain_rules", {}) or {}).get("rules", []):
            column = rule["column"]
            if column not in source_types or column not in target_types:
                raise ValidationError("도메인 규칙 컬럼이 원천과 대상에 없습니다: %s" % column)
            if rule["action"] == "fill":
                current = column_expressions[column]
                condition = domain_condition_expression(rule, current)
                column_expressions[column] = "CASE WHEN %s THEN %s ELSE %s END" % (
                    condition, sql_literal(rule["value"]), current
                )

    for column in target_order:
        expression = column_expressions[column]
        quoted = quote_identifier(column)
        expressions.append("%s AS %s" % (expression, quoted))

    noise_condition = build_noise_condition(table, source_types)
    domain_exclusion = build_domain_exclusion_condition(
        table, source_types, column_expressions
    )
    retained_condition = combine_conditions(
        source_condition,
        "NOT (%s)" % noise_condition if noise_condition else None,
        "NOT (%s)" % domain_exclusion if domain_exclusion else None,
    )
    normalized = "SELECT\n    %s\nFROM %s%s" % (
        ",\n    ".join(expressions),
        quote_table(table["source"]),
        "\nWHERE %s" % retained_condition if retained_condition else "",
    )

    deduplicate = table.get("deduplicate", {}) or {}
    dedup_keys: List[str] = []
    if deduplicate.get("enabled"):
        dedup_keys = deduplication_keys(deduplicate, target_primary_keys)
        order_by = deduplicate.get("order_by") or []
        if not order_by:
            raise ValidationError("중복 제거에는 결정적인 order_by 설정이 필요합니다.")
        for column in dedup_keys:
            if column not in target_types:
                raise ValidationError("중복 제거 키가 대상에 없습니다: %s" % column)
        for item in order_by:
            if item["column"] not in target_types:
                raise ValidationError("중복 정렬 컬럼이 대상에 없습니다: %s" % item["column"])

        partition_sql = ", ".join(quote_identifier(item) for item in dedup_keys)
        order_sql = ", ".join(
            "%s %s" % (quote_identifier(item["column"]), item.get("direction", "desc").upper())
            for item in order_by
        )
        selected = ", ".join(quote_identifier(name) for name in target_order)
        # CTE를 INSERT나 COUNT 내부에 중첩하면 Impala 버전별 문법 차이가 생길
        # 수 있어, 어디서나 재사용하기 쉬운 일반 서브쿼리로 조합한다.
        transform = (
            "SELECT %s FROM (\n"
            "SELECT normalized.*, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) AS __row_num\n"
            "FROM (\n%s\n) normalized\n"
            ") ranked WHERE __row_num = 1"
            % (selected, partition_sql, order_sql, normalized)
        )
    else:
        transform = normalized

    rule_detail = {
        "trim": {
            "enabled": trim_enabled,
            "exclude_columns": sorted(excluded),
            "columns": table.get("trim", {}).get("columns"),
        },
        "datetime": table.get("datetime", []) or [],
        "deduplicate": deduplicate,
        "noise_filters": table.get("noise_filters", {}) or {},
        "domain_rules": table.get("domain_rules", {}) or {},
        "domain_rules_execution_enabled": domain_rules_active(table),
    }
    return transform, target_order, trimmed_columns, rule_detail


def build_invalid_datetime_count_sql(
    table: Dict[str, Any], invalid_action: Optional[str] = None,
    source_condition: Optional[str] = None,
    source_types: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    """입력값은 있는데 어떤 지정 포맷으로도 변환되지 않는 행을 센다.

    invalid_action을 주면 그 정책을 가진 날짜 규칙만 검사한다.
    """
    invalid_conditions: List[str] = []
    for rule in table.get("datetime", []) or []:
        if invalid_action and rule.get("invalid_action", "null") != invalid_action:
            continue
        column = rule["column"]
        source_type = (source_types or {}).get(column, "string")
        # 타입이 TIMESTAMP인 값은 포맷 파싱에 실패할 수 없으므로 변환 실패
        # 집계 대상에서 제외한다. NULL은 별도의 결측 규칙으로 관리한다.
        if source_type.startswith("timestamp"):
            continue
        expression = datetime_expression(column, rule, source_type)
        source = "TRIM(%s)" % quote_identifier(column)
        invalid_conditions.append("(%s IS NOT NULL AND %s <> '' AND %s IS NULL)" % (source, source, expression))
    if not invalid_conditions:
        return None
    conditions = list(invalid_conditions)
    if source_condition:
        conditions.insert(0, "(%s)" % source_condition)
    return "SELECT COUNT(*) FROM %s WHERE %s" % (
        quote_table(table["source"]),
        " AND ".join(
            [conditions[0], "(%s)" % " OR ".join(conditions[1:])]
            if source_condition else [" OR ".join(conditions)]
        ),
    )


def ensure_history_table(cursor: Any, history_table: str) -> None:
    """전처리 이력 Kudu 테이블이 없으면 최초 실행 시 생성한다."""
    table = quote_table(history_table, "history.table")
    sql = """
CREATE TABLE IF NOT EXISTS {table} (
    run_id STRING NOT NULL,
    target_table STRING NOT NULL,
    source_table STRING,
    status STRING,
    started_at STRING,
    finished_at STRING,
    elapsed_seconds DOUBLE,
    source_count BIGINT,
    processed_count BIGINT,
    target_count BIGINT,
    duplicate_removed_count BIGINT,
    noise_removed_count BIGINT,
    datetime_invalid_count BIGINT,
    rule_detail STRING,
    impala_query_id STRING,
    error_message STRING,
    PRIMARY KEY (run_id, target_table)
)
PARTITION BY HASH (run_id) PARTITIONS 3
STORED AS KUDU
""".format(table=table)
    execute(cursor, sql)
    # 기존 운영 테이블에는 신규 컬럼이 없을 수 있으므로 최초 업그레이드 때만
    # 추가한다. CREATE TABLE IF NOT EXISTS만으로는 기존 스키마가 변경되지 않는다.
    existing_columns = {column["name"] for column in describe_table(cursor, history_table)}
    if "noise_removed_count" not in existing_columns:
        execute(cursor, "ALTER TABLE %s ADD COLUMNS (noise_removed_count BIGINT)" % table)


def ensure_change_history_table(cursor: Any, change_table: str) -> None:
    """행·컬럼 단위 변경 내역을 저장할 Kudu 테이블을 생성한다."""
    sql = """
CREATE TABLE IF NOT EXISTS {table} (
    run_id STRING NOT NULL,
    target_table STRING NOT NULL,
    row_key STRING NOT NULL,
    column_name STRING NOT NULL,
    change_type STRING NOT NULL,
    source_table STRING,
    before_value STRING,
    after_value STRING,
    changed_at STRING,
    PRIMARY KEY (run_id, target_table, row_key, column_name, change_type)
)
PARTITION BY HASH (run_id) PARTITIONS 3
STORED AS KUDU
""".format(table=quote_table(change_table, "history.change_table"))
    execute(cursor, sql)


def row_key_expression(primary_keys: Sequence[str]) -> str:
    """복합 원천 PK를 사람이 읽을 수 있는 단일 문자열로 만든다."""
    if not primary_keys:
        raise ValidationError("행 변경 이력을 기록하려면 원천 Kudu PRIMARY KEY가 필요합니다.")
    parts = [
        "CONCAT(%s, '=', COALESCE(CAST(%s AS STRING), '<NULL>'))"
        % (sql_literal(column), quote_identifier(column))
        for column in primary_keys
    ]
    return "CONCAT_WS('|', %s)" % ", ".join(parts)


def transformed_column_expression(
    column: str,
    source_types: Dict[str, str],
    cleanup_rules: Dict[str, Dict[str, bool]],
    datetime_rules: Dict[str, Dict[str, Any]],
) -> str:
    """컬럼 하나에 실제 적용되는 전처리 표현식을 반환한다."""
    if column in datetime_rules:
        return datetime_expression(column, datetime_rules[column], source_types[column])
    if column in cleanup_rules:
        return text_cleanup_expression(quote_identifier(column), cleanup_rules[column])
    return quote_identifier(column)


def record_change_history(
    cursor: Any,
    change_table: str,
    table: Dict[str, Any],
    run_id: str,
    source_columns: Sequence[Dict[str, str]],
    source_primary_keys: Sequence[str],
    target_primary_keys: Sequence[str],
    source_condition: Optional[str] = None,
) -> None:
    """공백·날짜·중복·노이즈 처리 행을 원천 PK 기준으로 기록한다."""
    source_types = column_map(source_columns)
    datetime_rules = {rule["column"]: rule for rule in table.get("datetime", []) or []}
    cleanup_rules = trim_column_rules(table, source_types)
    row_key = row_key_expression(source_primary_keys)
    noise_condition = build_noise_condition(table, source_types)
    retained_condition = combine_conditions(
        source_condition,
        "NOT (%s)" % noise_condition if noise_condition else None,
    )
    changed_at = now_text()
    insert_prefix = (
        "INSERT INTO %s (run_id, target_table, row_key, column_name, change_type, "
        "source_table, before_value, after_value, changed_at) "
        % quote_table(change_table, "history.change_table")
    )

    # 공백·개행 전처리 결과가 실제로 달라진 컬럼만 한 건씩 남긴다.
    for column, options in cleanup_rules.items():
        if column in datetime_rules:
            continue
        quoted = quote_identifier(column)
        after_expression = text_cleanup_expression(quoted, options)
        change_type = (
            "TRIM"
            if options.get("trim")
            and not options.get("collapse_spaces")
            and not options.get("collapse_newlines")
            and not options.get("newline_to_space")
            else "TEXT_STANDARDIZED"
        )
        sql = insert_prefix + (
            "SELECT %s, %s, %s, %s, %s, %s, CAST(%s AS STRING), "
            "CAST(%s AS STRING), %s FROM %s "
            "WHERE %s IS NOT NULL AND CAST(%s AS STRING) <> CAST(%s AS STRING)%s"
            % (
                sql_literal(run_id), sql_literal(table["target"]), row_key,
                sql_literal(column), sql_literal(change_type), sql_literal(table["source"]),
                quoted, after_expression, sql_literal(changed_at), quote_table(table["source"]),
                quoted, quoted, after_expression,
                " AND (%s)" % retained_condition if retained_condition else "",
            )
        )
        execute(cursor, sql)

    # 날짜 문자열이 표준 출력과 다른 경우와 변환 실패(NULL)를 모두 기록한다.
    for column, rule in datetime_rules.items():
        quoted = quote_identifier(column)
        source_type = source_types[column]
        after_expression = datetime_expression(column, rule, source_type)
        if source_type.startswith("timestamp"):
            change_condition = (
                "%s IS NOT NULL AND CAST(%s AS STRING) <> %s"
                % (quoted, quoted, after_expression)
            )
        else:
            change_condition = (
                "TRIM(%s) <> '' AND (%s IS NULL OR TRIM(%s) <> %s)"
                % (quoted, after_expression, quoted, after_expression)
            )
        sql = insert_prefix + (
            "SELECT %s, %s, %s, %s, 'DATETIME_NORMALIZED', %s, "
            "CAST(%s AS STRING), CAST(%s AS STRING), %s FROM %s "
            "WHERE %s%s"
            % (
                sql_literal(run_id), sql_literal(table["target"]), row_key,
                sql_literal(column), sql_literal(table["source"]), quoted,
                after_expression, sql_literal(changed_at), quote_table(table["source"]),
                change_condition,
                " AND (%s)" % retained_condition if retained_condition else "",
            )
        )
        execute(cursor, sql)

    # 중복으로 제외된 각 원천 행도 원천 PK를 남겨 재현할 수 있게 한다.
    deduplicate = table.get("deduplicate", {}) or {}
    if deduplicate.get("enabled"):
        keys = deduplication_keys(deduplicate, target_primary_keys)
        order_by = deduplicate.get("order_by") or []
        partition_sql = ", ".join(
            transformed_column_expression(
                column, source_types, cleanup_rules, datetime_rules
            )
            for column in keys
        )
        order_sql = ", ".join(
            "%s %s"
            % (
                transformed_column_expression(
                    item["column"], source_types, cleanup_rules, datetime_rules
                ),
                item.get("direction", "desc").upper(),
            )
            for item in order_by
        )
        sql = insert_prefix + (
            "SELECT %s, %s, row_key, '', 'DUPLICATE_REMOVED', %s, NULL, NULL, %s "
            "FROM (SELECT %s AS row_key, ROW_NUMBER() OVER "
            "(PARTITION BY %s ORDER BY %s) AS row_num FROM %s%s) ranked "
            "WHERE row_num > 1"
            % (
                sql_literal(run_id), sql_literal(table["target"]),
                sql_literal(table["source"]), sql_literal(changed_at), row_key,
                partition_sql, order_sql, quote_table(table["source"]),
                " WHERE %s" % retained_condition if retained_condition else "",
            )
        )
        execute(cursor, sql)

    # 노이즈 규칙별로 제외된 행과 판별값을 남긴다. 같은 행이 여러 규칙과
    # 일치할 수 있으므로 change_type에 규칙명을 붙여 PK 충돌을 방지한다.
    noise = table.get("noise_filters", {}) or {}
    if noise.get("enabled"):
        for rule in noise.get("rules", []):
            column = quote_identifier(rule["column"])
            rule_condition = noise_rule_expression(rule, source_types)
            condition = combine_conditions(source_condition, rule_condition)
            sql = insert_prefix + (
                "SELECT %s, %s, %s, %s, %s, %s, CAST(%s AS STRING), NULL, %s "
                "FROM %s WHERE %s"
                % (
                    sql_literal(run_id), sql_literal(table["target"]), row_key,
                    sql_literal(rule["column"]),
                    sql_literal("NOISE_REMOVED:%s" % rule["name"]),
                    sql_literal(table["source"]), column, sql_literal(changed_at),
                    quote_table(table["source"]), condition,
                )
            )
            execute(cursor, sql)


def write_history(cursor: Any, history_table: str, record: Dict[str, Any]) -> None:
    """같은 run_id와 target_table 이력을 UPSERT하여 실행 상태를 갱신한다."""
    columns = [
        "run_id", "target_table", "source_table", "status", "started_at", "finished_at",
        "elapsed_seconds", "source_count", "processed_count", "target_count",
        "duplicate_removed_count", "noise_removed_count", "datetime_invalid_count", "rule_detail",
        "impala_query_id", "error_message",
    ]
    values = ", ".join(sql_literal(record.get(column)) for column in columns)
    column_sql = ", ".join(quote_identifier(column) for column in columns)
    execute(
        cursor,
        "UPSERT INTO %s (%s) VALUES (%s)"
        % (quote_table(history_table), column_sql, values),
    )


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def select_tables(config: Dict[str, Any], selected_name: Optional[str]) -> List[Dict[str, Any]]:
    tables: List[Dict[str, Any]] = []
    names = set()
    for table in config["tables"]:
        if not isinstance(table, dict):
            raise ConfigError("tables 항목은 객체여야 합니다.")
        validate_table_config(table)
        if table["name"] in names:
            raise ConfigError("중복된 테이블 name입니다: %s" % table["name"])
        names.add(table["name"])
        if table.get("enabled", True) and (not selected_name or table["name"] == selected_name):
            tables.append(table)
    if selected_name and not tables:
        raise ConfigError("활성화된 테이블 설정을 찾을 수 없습니다: %s" % selected_name)
    if not tables:
        raise ConfigError("실행할 활성 테이블이 없습니다.")
    return tables


def run_table(
    cursor: Any,
    table: Dict[str, Any],
    history_table: str,
    change_history_table: str,
    run_id: str,
    dry_run: bool,
    validate_only: bool,
) -> None:
    """테이블 하나를 전체 또는 날짜 구간으로 전처리하고 이력을 남긴다."""
    started = time.monotonic()
    started_at = now_text()
    source = table["source"]
    target = table["target"]
    record: Dict[str, Any] = {
        "run_id": run_id,
        "source_table": source,
        "target_table": target,
        "status": "RUNNING",
        "started_at": started_at,
    }

    LOGGER.info("run_id=%s table=%s source=%s target=%s status=STARTED", run_id, table["name"], source, target)

    try:
        source_columns = describe_table(cursor, source)
        target_columns = describe_table(cursor, target)
        source_primary_keys = get_primary_keys(cursor, source)
        target_primary_keys = get_primary_keys(cursor, target)
        # 컬럼 호환성과 전처리 규칙은 날짜 분할 여부와 무관하므로 먼저 한 번
        # 검증한다. 실제 구간 조건이 붙은 SQL은 아래에서 구간별로 생성한다.
        _, target_order, trimmed_columns, rule_detail = build_transform_sql(
            table, source_columns, target_columns, target_primary_keys
        )
        load = table.get("load", {}) or {}
        load_mode = load.get("mode", "full")
        rule_detail["load"] = load or {"mode": "full"}
        record["rule_detail"] = json.dumps(rule_detail, ensure_ascii=False, separators=(",", ":"))

        source_types = column_map(source_columns)
        target_types = column_map(target_columns)
        if load_mode == "date_range":
            date_column = load["date_column"]
            if date_column not in source_types or date_column not in target_types:
                raise ValidationError(
                    "날짜 분할 컬럼은 원천과 대상에 모두 있어야 합니다: %s" % date_column
                )
            allowed_types = ("string", "varchar", "char", "date", "timestamp")
            if not source_types[date_column].startswith(allowed_types):
                raise ValidationError("원천 날짜 분할 컬럼 타입을 지원하지 않습니다: %s" % date_column)
            if not target_types[date_column].startswith(allowed_types):
                raise ValidationError("대상 날짜 분할 컬럼 타입을 지원하지 않습니다: %s" % date_column)

        if not dry_run and not validate_only:
            write_history(cursor, history_table, record)

        if validate_only:
            LOGGER.info(
                "run_id=%s table=%s validation=PASSED trim_columns=%s",
                run_id, table["name"], ",".join(trimmed_columns),
            )
            return

        plans: List[Dict[str, Any]] = []

        if load_mode == "date_range":
            date_column = load["date_column"]
            for chunk_start, chunk_end in build_date_ranges(load):
                source_format = load.get("source_format", "yyyy-MM-dd HH:mm:ss")
                target_format = load.get("target_format", "yyyy-MM-dd HH:mm:ss")
                # DATE/TIMESTAMP는 타입에 맞는 표준 리터럴을 사용한다. format
                # 설정은 문자열 날짜 컬럼에만 적용된다.
                if source_types[date_column].startswith("date"):
                    source_format = "yyyy-MM-dd"
                elif source_types[date_column].startswith("timestamp"):
                    source_format = "yyyy-MM-dd HH:mm:ss"
                if target_types[date_column].startswith("date"):
                    target_format = "yyyy-MM-dd"
                elif target_types[date_column].startswith("timestamp"):
                    target_format = "yyyy-MM-dd HH:mm:ss"
                plans.append(
                    {
                        "start": chunk_start,
                        "end": chunk_end,
                        "source_condition": build_range_condition(
                            date_column, chunk_start, chunk_end, source_format,
                            source_types[date_column],
                        ),
                        "target_condition": build_range_condition(
                            date_column, chunk_start, chunk_end, target_format,
                            target_types[date_column],
                        ),
                    }
                )
        else:
            plans.append({"start": None, "end": None, "source_condition": None, "target_condition": None})

        # 모든 구간을 먼저 검증하여 뒤쪽 구간의 데이터 오류 때문에 앞쪽 구간만
        # 갱신되는 상황을 최대한 방지한다. Kudu DML 자체는 트랜잭션이 아니다.
        source_count = 0
        processed_count = 0
        noise_removed_count = 0
        invalid_datetime_count = 0
        for plan in plans:
            condition = plan["source_condition"]
            noise_condition = build_noise_condition(table, source_types)
            retained_condition = combine_conditions(
                condition,
                "NOT (%s)" % noise_condition if noise_condition else None,
            )
            transform_sql, _, _, _ = build_transform_sql(
                table, source_columns, target_columns, target_primary_keys, condition
            )
            plan["transform_sql"] = transform_sql
            source_count_sql = "SELECT COUNT(*) FROM %s%s" % (
                quote_table(source), " WHERE %s" % condition if condition else ""
            )
            plan["source_count"] = scalar(cursor, source_count_sql)
            plan["processed_count"] = scalar(
                cursor, "SELECT COUNT(*) FROM (%s) processed" % transform_sql
            )
            if noise_condition:
                noise_count_condition = combine_conditions(condition, noise_condition)
                plan["noise_count"] = scalar(
                    cursor,
                    "SELECT COUNT(*) FROM %s WHERE %s"
                    % (quote_table(source), noise_count_condition),
                )
            else:
                plan["noise_count"] = 0
            invalid_sql = build_invalid_datetime_count_sql(
                table,
                source_condition=retained_condition,
                source_types=source_types,
            )
            plan["invalid_count"] = scalar(cursor, invalid_sql) if invalid_sql else 0
            source_count += plan["source_count"]
            processed_count += plan["processed_count"]
            noise_removed_count += plan["noise_count"]
            invalid_datetime_count += plan["invalid_count"]

            if target_primary_keys:
                null_condition = " OR ".join(
                    "%s IS NULL" % quote_identifier(column) for column in target_primary_keys
                )
                null_key_count = scalar(
                    cursor,
                    "SELECT COUNT(*) FROM (%s) processed WHERE %s"
                    % (transform_sql, null_condition),
                )
                if null_key_count:
                    raise ValidationError(
                        "대상 Kudu PRIMARY KEY가 NULL인 데이터가 %d건 있습니다."
                        % null_key_count
                    )

            fail_invalid_sql = build_invalid_datetime_count_sql(
                table,
                invalid_action="fail",
                source_condition=retained_condition,
                source_types=source_types,
            )
            fail_invalid_count = scalar(cursor, fail_invalid_sql) if fail_invalid_sql else 0
            if fail_invalid_count:
                fail_columns = [
                    rule["column"]
                    for rule in table.get("datetime", []) or []
                    if rule.get("invalid_action", "null") == "fail"
                ]
                raise ValidationError(
                    "날짜 변환 실패가 %d건 있습니다. fail 대상 컬럼=%s"
                    % (fail_invalid_count, ",".join(fail_columns))
                )

        # processed_count는 노이즈 제외 후 중복 제거 결과다. 두 사유의 건수를
        # 분리하여 같은 제외 행을 중복 건수로 다시 세지 않는다.
        duplicate_removed_count = source_count - noise_removed_count - processed_count

        record.update(
            {
                "source_count": source_count,
                "processed_count": processed_count,
                "duplicate_removed_count": duplicate_removed_count,
                "noise_removed_count": noise_removed_count,
                "datetime_invalid_count": invalid_datetime_count,
            }
        )

        validation = table.get("validation", {}) or {}
        if source_count == 0 and not validation.get("allow_empty", False):
            raise ValidationError("원천 데이터가 0건이므로 대상 갱신을 중단합니다.")
        reduction_percent = ((source_count - processed_count) / source_count * 100.0) if source_count else 0.0
        max_reduction = float(validation.get("max_reduction_percent", 100.0))
        if reduction_percent > max_reduction:
            raise ValidationError(
                "데이터 감소율 %.2f%%가 허용값 %.2f%%를 초과했습니다." % (reduction_percent, max_reduction)
            )

        LOGGER.info(
            "run_id=%s table=%s load_mode=%s chunks=%d source_count=%d processed_count=%d "
            "duplicate_removed_count=%d noise_removed_count=%d datetime_invalid_count=%d "
            "reduction_percent=%.2f trim_columns=%s",
            run_id, table["name"], load_mode, len(plans), source_count, processed_count, duplicate_removed_count,
            noise_removed_count, invalid_datetime_count, reduction_percent, ",".join(trimmed_columns),
        )

        if dry_run:
            LOGGER.info("run_id=%s table=%s status=DRY_RUN_COMPLETE", run_id, table["name"])
            return

        target_column_sql = ", ".join(quote_identifier(name) for name in target_order)
        target_count = 0
        query_id = None
        for index, plan in enumerate(plans, start=1):
            condition = plan["source_condition"]
            target_condition = plan["target_condition"]
            range_label = (
                "%s~%s" % (plan["start"].strftime("%Y-%m-%d"), plan["end"].strftime("%Y-%m-%d"))
                if plan["start"] else "FULL"
            )
            LOGGER.info(
                "run_id=%s table=%s chunk=%d/%d range=%s status=STARTED",
                run_id, table["name"], index, len(plans), range_label,
            )
            # 대상 데이터를 바꾸기 전에 이 구간에서 변경·제외되는 원천 행을
            # 상세 이력에 남긴다.
            record_change_history(
                cursor, change_history_table, table, run_id, source_columns,
                source_primary_keys, target_primary_keys, condition,
            )
            delete_sql = "DELETE FROM %s" % quote_table(target)
            if target_condition:
                delete_sql += " WHERE %s" % target_condition
            execute(cursor, delete_sql)
            execute(
                cursor,
                "INSERT INTO %s (%s) %s"
                % (quote_table(target), target_column_sql, plan["transform_sql"]),
            )
            query_id = get_query_id(cursor) or query_id
            target_count_sql = "SELECT COUNT(*) FROM %s%s" % (
                quote_table(target),
                " WHERE %s" % target_condition if target_condition else "",
            )
            chunk_target_count = scalar(cursor, target_count_sql)
            if chunk_target_count != plan["processed_count"]:
                raise ValidationError(
                    "구간 적재 건수 검증 실패: range=%s, processed_count=%d, target_count=%d"
                    % (range_label, plan["processed_count"], chunk_target_count)
                )
            target_count += chunk_target_count
            LOGGER.info(
                "run_id=%s table=%s chunk=%d/%d range=%s status=SUCCESS target_count=%d",
                run_id, table["name"], index, len(plans), range_label, chunk_target_count,
            )

        record.update(
            {
                "status": "SUCCESS",
                "finished_at": now_text(),
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "target_count": target_count,
                "impala_query_id": query_id,
                "error_message": None,
            }
        )
        write_history(cursor, history_table, record)
        LOGGER.info(
            "run_id=%s table=%s status=SUCCESS target_count=%d elapsed_seconds=%.3f query_id=%s",
            run_id, table["name"], target_count, record["elapsed_seconds"], query_id or "unknown",
        )
    except Exception as exc:
        record.update(
            {
                "status": "FAILED",
                "finished_at": now_text(),
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "impala_query_id": get_query_id(cursor),
                "error_message": str(exc)[:4000],
            }
        )
        if not dry_run and not validate_only:
            try:
                write_history(cursor, history_table, record)
            except Exception as history_exc:
                LOGGER.error("run_id=%s history_write_failed=%s", run_id, history_exc)
        LOGGER.error("run_id=%s table=%s status=FAILED error=%s", run_id, table["name"], exc)
        raise


def main() -> int:
    args = parse_args()
    config_path = Path(args.config).resolve()
    try:
        config = load_config(config_path)
    except Exception as exc:
        print("설정 파일을 읽을 수 없습니다: %s" % exc, file=sys.stderr)
        return 2
    project_dir = config_path.parent

    log_dir = Path(config["runtime"]["log_dir"])
    if not log_dir.is_absolute():
        log_dir = project_dir / log_dir
    setup_logging(log_dir, args.debug)

    lock_path = Path(config["runtime"]["lock_file"])
    if not lock_path.is_absolute():
        lock_path = project_dir / lock_path

    run_id = "%s_%s" % (datetime.now().strftime("%Y%m%d_%H%M%S"), uuid.uuid4().hex[:8])
    mode = "validate" if args.validate_only else ("dry-run" if args.dry_run else "execute")
    LOGGER.info(
        "run_id=%s batch_started mode=%s host=%s python=%s",
        run_id, mode, socket.gethostname(), sys.version.split()[0],
    )

    connection = None
    kerberos_state = None
    try:
        tables = select_tables(config, args.table)
        with ProcessLock(lock_path):
            kerberos_state = prepare_kerberos(config, project_dir)
            connection = connect_impala(config["impala"])
            cursor = connection.cursor()
            # 소켓 생성만 성공하고 GSSAPI 협상은 첫 SQL에서 실패하는 경우가
            # 있으므로 실제 쿼리로 인증과 세션을 즉시 확인한다.
            execute(cursor, "SELECT 1", fetch=True)
            LOGGER.info("impala_connection_status=VERIFIED auth_mechanism=%s", config["impala"].get("auth_mechanism", "GSSAPI"))
            history_table = config["history"]["table"]
            change_history_table = config["history"]["change_table"]
            if not args.dry_run and not args.validate_only:
                ensure_history_table(cursor, history_table)
                ensure_change_history_table(cursor, change_history_table)
            for table in tables:
                ensure_kerberos_ticket(kerberos_state)
                # 기본값은 False다. config.yaml의 기능 스위치를 명시적으로
                # 활성화한 경우에만 domain_rules를 실제 SQL에 연결한다.
                table["_domain_rules_execution_enabled"] = bool(
                    config.get("features", {}).get("domain_rules_enabled", False)
                )
                if table.get("domain_rules", {}).get("enabled") and not table["_domain_rules_execution_enabled"]:
                    LOGGER.info(
                        "run_id=%s table=%s domain_rules_status=DISABLED feature_switch=false",
                        run_id, table["name"],
                    )
                run_table(
                    cursor,
                    table,
                    history_table,
                    change_history_table,
                    run_id,
                    args.dry_run,
                    args.validate_only,
                )
            cursor.close()
        LOGGER.info("run_id=%s batch_status=SUCCESS", run_id)
        return 0
    except ConfigError as exc:
        LOGGER.error("run_id=%s batch_status=FAILED error_type=CONFIG error=%s", run_id, exc)
        return 2
    except KerberosError as exc:
        LOGGER.error("run_id=%s batch_status=FAILED error_type=KERBEROS error=%s", run_id, exc)
        return 3
    except ValidationError as exc:
        LOGGER.error("run_id=%s batch_status=FAILED error_type=VALIDATION error=%s", run_id, exc)
        return 5
    except Exception as exc:
        LOGGER.error("run_id=%s batch_status=FAILED error_type=RUNTIME error=%s", run_id, exc)
        LOGGER.debug("traceback=%s", traceback.format_exc())
        return 1
    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                LOGGER.warning("run_id=%s impala_connection_close_failed=true", run_id)
        cleanup_kerberos(kerberos_state)


if __name__ == "__main__":
    sys.exit(main())
