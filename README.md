# AI Lake Preprocessing

CDP의 `ai_lake` Kudu 테이블을 Impala SQL로 전처리하여
`ai_datawarehouse` Kudu 테이블에 적재하는 소규모 배치 프로그램입니다.
일반 테이블은 전체 적재하고, 빅테이블은 지정한 날짜 범위를 일정한 크기로
나누어 적재할 수 있습니다.

Python은 데이터를 collect 서버 메모리로 가져오지 않습니다. Python은 설정을
검증하고 Impala에 SQL을 제출하며, 실제 대용량 처리는 Impala와 Kudu에서
수행됩니다.

## 지원 환경

- collect 서버: Linux 온프레미스 환경
- Python: **3.8.12**
- 인증: Kerberos/GSSAPI
- 저장소: Kudu
- SQL 엔진: Impala
- 스케줄: 초기 수동 실행, 안정화 후 cron

Spark, Pandas, Oozie, Airflow는 사용하지 않습니다.

## 프로젝트 구성

```text
ai_lake_preprocessing/
├─ main.py
├─ config.yaml
├─ requirements.txt
├─ run_batch.sh
├─ sql/
│  └─ preprocessing_rule.sql
├─ library_bsn/
│  ├─ windows/
│  └─ linux/
├─ logs/
└─ run/
```

- `main.py`: 설정 검증, SQL 생성, Impala 실행, 검증 및 이력 기록
- `config.yaml`: Impala 연결과 테이블별 전처리 규칙
- `run_batch.sh`: Kerberos 인증 후 Python을 실행하는 공통 진입점
- `sql/preprocessing_rule.sql`: YAML 기반 전처리 규칙 Kudu 테이블 생성·적재 SQL
- `library_bsn`: 폐쇄망 설치용 일반 Python wheel
- `logs`: 날짜별 실행 로그
- `run`: 중복 실행 방지 잠금 파일

## 전처리 범위

### 문자열 앞뒤 공백 제거

원천의 `STRING`, `VARCHAR`, `CHAR` 컬럼에 컬럼별 공백·개행 규칙을 적용할 수
있습니다. `columns`를 작성하면 목록에 지정한 활성 컬럼만 처리합니다. JSON
원문과 같은 `payload` 컬럼은 지정하지 않거나 `exclude_columns`에 넣어 원본을
보존합니다.

`trim`은 각 `tables` 항목 안에 작성하므로 테이블별로 독립적으로 설정됩니다.
상위 항목의 `source`가 읽을 테이블이고 `target`이 적재할 테이블입니다.
`trim` 안에 테이블명을 중복해서 작성할 필요는 없습니다.

```yaml
tables:
  - name: customer
    source: ai_lake.customer
    target: ai_datawarehouse.customer
    trim:
      # customer 테이블에만 적용
      enabled: true
      columns:
        - column: customer_name
          trim: true

  - name: order
    source: ai_lake.order
    target: ai_datawarehouse.order
    trim:
      # order 테이블에만 적용
      enabled: true
      columns:
        - column: order_name
          trim: true
          collapse_spaces: true
```

```yaml
trim:
  enabled: true
  exclude_columns:
    - payload
  columns:
    - column: customer_name
      enabled: true
      trim: true
      collapse_spaces: true
      collapse_newlines: false
      newline_to_space: true

    - column: description
      enabled: true
      trim: true
      collapse_spaces: false
      collapse_newlines: true
      newline_to_space: false
```

옵션별 동작은 다음과 같습니다.

| 옵션 | 처리 내용 |
|---|---|
| `enabled` | 해당 컬럼의 문자열 전처리 활성화 여부 |
| `trim` | 문자열 앞뒤 공백 제거 |
| `collapse_spaces` | 연속된 일반 공백을 한 칸으로 변환 |
| `collapse_newlines` | CRLF·CR·LF를 LF로 통일하고 연속 개행을 하나로 변환 |
| `newline_to_space` | CR과 LF를 일반 공백으로 변환 |

`collapse_newlines`와 `newline_to_space`는 개행 보존 여부가 서로 반대이므로
동시에 `true`로 설정할 수 없습니다. `newline_to_space`와
`collapse_spaces`를 함께 사용하면 개행을 공백으로 바꾼 뒤 연속 공백도 한
칸으로 줄입니다.

`columns`를 생략하면 이전 설정과의 호환을 위해 `exclude_columns`를 제외한
모든 문자열 컬럼에 양끝 `TRIM`만 적용합니다. 처리된 변경은 단순 양끝
공백 제거인 경우 `TRIM`, 그 외 공백·개행 표준화는 `TEXT_STANDARDIZED`로
상세 변경이력에 기록됩니다.

### 날짜 표준화

YAML에 명시한 컬럼만 날짜 변환합니다. 설정하지 않은 문자열이나 TIMESTAMP는
변경하지 않습니다. 다음 두 조합을 지원합니다.

- 원천 `STRING` → 대상 `STRING`: `input_formats`로 파싱한 뒤 문자열로 저장
- 원천 `TIMESTAMP` → 대상 `STRING`: 파싱 없이 `output_format`으로 문자열 저장

```yaml
datetime:
  - column: created_at
    input_formats:
      - yyyyMMddHHmmss
      - yyyy-MM-dd HH:mm:ss
      - yyyy/MM/dd HH:mm:ss
    output_format: yyyy-MM-dd HH:mm:ss
    invalid_action: null
```

입력 포맷은 위에서 아래 순서로 적용합니다. 출력은 예를 들어
`2026-10-12 14:30:15` 형태의 STRING입니다.

원천 TIMESTAMP를 DW에 `2026-01-01T10:00:00` 형태의 문자열로 저장하려면
DW의 같은 컬럼을 `STRING`으로 만들고 다음처럼 설정합니다.

```yaml
datetime:
  - column: created_at
    output_format: yyyy-MM-dd'T'HH:mm:ss
    invalid_action: fail
```

이 경우 생성되는 Impala SQL은 `FROM_TIMESTAMP()`로 TIMESTAMP를 직접
포맷하므로 `input_formats`가 필요하지 않습니다. TIMESTAMP 원천은 이미 유효한
날짜 타입이므로 날짜 문자열 파싱 실패 건수에는 포함하지 않고, NULL 처리는
결측 규칙으로 관리합니다. 날짜 규칙에 없는 일반 컬럼은 기존처럼 원천과 대상
타입이 같아야 합니다.

- `invalid_action: null`: 변환할 수 없는 값을 NULL로 저장
- `invalid_action: fail`: 변환 실패가 있으면 대상 변경 전에 작업 실패

Impala 배포판에 따라 날짜 포맷 함수의 세부 문법이 다를 수 있으므로 개발계에서
반드시 실제 CDP/Impala 버전으로 검증해야 합니다.

### 중복 제거

중복 제거가 필요한 테이블에만 활성화합니다.

```yaml
deduplicate:
  enabled: true
  allow_target_primary_key_fallback: false
  keys:
    - accident_number
    - accident_datetime
    - accident_location_code
  order_by:
    - column: updated_at
      direction: desc
    - column: source_sequence
      direction: asc
```

`ROW_NUMBER()`로 업무 키별 한 행을 선택합니다. `keys`에는 Kudu PK가 아니라
업무적으로 동일 데이터인지 판단할 컬럼을 명시하는 것이 기본입니다. 예를 들어
동일 사고 기준이라면 사고번호·발생일시·장소코드의 조합을 사용할 수 있습니다.

원본 Kudu PK는 이미 행마다 고유하므로 PK만 사용하면 업무상 같은 사고가 서로
다른 행으로 남을 수 있습니다. 이런 실수를 방지하기 위해 `keys`는 기본적으로
필수입니다. 정말 대상 Kudu PK를 중복 기준으로 사용해야 하는 테이블만 다음
설정을 명시적으로 사용합니다.

```yaml
deduplicate:
  enabled: true
  allow_target_primary_key_fallback: true
  # keys를 의도적으로 생략
  order_by:
    - column: updated_at
      direction: desc
```

`order_by`는 중복 그룹에서 어느 행을 남길지 결정합니다. 결과가 실행할 때마다
달라지지 않도록 최종 정렬 컬럼에는 수집 순번이나 원본 PK처럼 동률을 해소할 수
있는 컬럼을 넣는 것이 좋습니다. 중복 제거가 필요하지 않으면 설정을 생략하거나
`enabled: false`로 지정합니다.

### 노이즈 필터링

테스트 데이터나 업무상 명백한 오기입 값을 YAML 규칙으로 판별하여 DW 적재에서
제외할 수 있습니다. 여러 규칙 중 하나라도 일치하면 행 전체를 제외합니다.

```yaml
noise_filters:
  enabled: true
  rules:
    - name: test_customer
      column: customer_name
      operator: contains
      value: test
      case_sensitive: false
      action: exclude

    - name: invalid_status_code
      column: status_code
      operator: in
      values:
        - XXX
        - UNKNOWN
      case_sensitive: false
      action: exclude
```

지원하는 `operator`는 다음과 같습니다.

| operator | 판별 방식 |
|---|---|
| `equals` | 값이 정확히 일치 |
| `not_equals` | 값이 일치하지 않음 |
| `contains` | 지정 문자열 포함 |
| `starts_with` | 지정 문자열로 시작 |
| `ends_with` | 지정 문자열로 끝남 |
| `regex` | Impala 정규식과 일치 |
| `in` | `values` 목록 중 하나와 일치 |

`case_sensitive: false`가 기본값이며 영문 대소문자를 구분하지 않습니다. 현재
처리 방식은 `action: exclude`만 지원합니다. 노이즈 판별에 사용한 값은
`preprocessing_change_history`에 기록되고, `change_type`은
`NOISE_REMOVED:규칙명`으로 저장됩니다. 실행별 전체 제외 건수는
`preprocessing_history.noise_removed_count`에 기록됩니다.

YAML의 컬럼명과 값은 예시를 그대로 활성화하면 안 됩니다. 실제 원천 테이블과
업무 정의를 확인하고, 정상 데이터가 제외되지 않는지 `--dry-run` 결과와 개발계
이력을 먼저 확인해야 합니다.

## 적재 방식

테이블마다 `load.mode`를 선택할 수 있습니다. 설정을 생략하면 `full`입니다.

### 일반 테이블 전체 적재

```yaml
load:
  mode: full
```

원천 전체를 전처리하고 대상 테이블 전체를 교체합니다. 대상 전체를 `DELETE`한
후 전처리된 결과를 `INSERT`합니다.

### 빅테이블 날짜 분할 적재

```yaml
load:
  mode: date_range
  date_column: created_at
  start_date: "2026-01-01"
  end_date: "2026-02-01"
  interval_days: 1
  source_format: yyyy-MM-dd HH:mm:ss
  target_format: yyyy-MM-dd HH:mm:ss
```

이 예시는 `2026-01-01` 이상부터 `2026-02-01` 미만까지 하루 단위로 나눕니다.
날짜 구간은 항상 `[start_date, end_date)` 형태입니다. 즉, 시작일은 포함하고
종료일은 포함하지 않습니다. 1월 한 달을 처리하려면 종료일을 `2026-02-01`로
지정해야 합니다.

각 구간은 다음 순서로 처리합니다.

1. 해당 구간의 원천 데이터 조회 및 전처리 예상 건수 검증
2. 모든 구간의 사전 검증 완료
3. 구간별 행·컬럼 변경이력 기록
4. DW에서 동일한 날짜 구간만 `DELETE`
5. 전처리 결과를 해당 구간에 `INSERT`
6. 구간별 대상 건수 검증

예를 들어 첫 번째 구간의 원천 및 대상 조건은 다음 형태입니다.

```sql
WHERE created_at >= '2026-01-01 00:00:00'
  AND created_at <  '2026-01-02 00:00:00'
```

`date_column`은 원천과 대상 양쪽에 모두 있어야 하며 `STRING`, `VARCHAR`,
`CHAR`, `DATE`, `TIMESTAMP` 타입을 지원합니다. 문자열 날짜에는 다음 네 형식만
지원합니다.

- `yyyy-MM-dd`
- `yyyy-MM-dd HH:mm:ss`
- `yyyyMMdd`
- `yyyyMMddHHmmss`

`source_format`은 ai_lake의 실제 저장 형식이고, `target_format`은 DW에 저장된
형식입니다. 날짜 표준화로 원천 `yyyyMMddHHmmss`를 DW
`yyyy-MM-dd HH:mm:ss`로 바꾸는 경우에는 다음처럼 서로 다르게 지정합니다.

```yaml
load:
  mode: date_range
  date_column: created_at
  start_date: "2026-01-01"
  end_date: "2026-02-01"
  interval_days: 1
  source_format: yyyyMMddHHmmss
  target_format: yyyy-MM-dd HH:mm:ss

datetime:
  - column: created_at
    input_formats:
      - yyyyMMddHHmmss
    output_format: yyyy-MM-dd HH:mm:ss
    invalid_action: fail
```

`DATE`와 `TIMESTAMP` 타입은 코드가 타입에 맞는 표준 경계값을 사용하므로
`source_format`과 `target_format` 값의 영향을 받지 않습니다.

문자열 날짜 분할 시에는 다음 제약을 반드시 확인합니다.

- 날짜 컬럼 전체가 설정한 한 가지 형식으로 저장되어 있어야 합니다.
- 지원 형식처럼 문자열 정렬 순서와 시간 순서가 같아야 합니다.
- 분할 조건은 전처리 전 원천 값에 적용되므로 여러 날짜 형식이 섞인 컬럼을
  분할 기준으로 사용하면 안 됩니다.
- Kudu 파티션 또는 정렬 설계에 날짜 컬럼이 반영되어 있어야 전체 스캔을 줄이는
  효과를 얻을 수 있습니다.
- 한 행의 날짜 분할 값은 적재 후 다른 날짜 구간으로 이동하지 않아야 합니다.
  동일 PK의 날짜 값이 바뀔 수 있으면 이전 날짜 구간의 행이 남거나 Kudu PK
  충돌이 생길 수 있으므로 해당 테이블은 전체 적재 또는 별도의 증분 병합 방식이
  필요합니다.
- 중복 제거는 각 날짜 구간 안에서 수행됩니다. 같은 중복 키가 여러 날짜 구간에
  걸칠 수 있다면 날짜 분할 적재를 사용하기 전에 중복 기준을 다시 설계해야 합니다.

날짜 범위 실행이 실패하면 `FAILED` 이력과 마지막 성공 구간 로그를 확인합니다.
Kudu DML은 하나의 트랜잭션이 아니므로 일부 앞쪽 구간이 이미 반영되었을 수
있습니다. 동일한 날짜 범위를 재실행하면 각 구간을 다시 삭제하고 적재하므로
결과를 복구할 수 있습니다. 범위를 줄여 재실행하려면 YAML의 `start_date`와
`end_date`를 실패 구간에 맞춰 임시로 조정합니다.

## 적재 안전장치

업무 대상 Kudu 테이블은 프로그램 실행 전에 DBA 또는 담당자가 생성해야 합니다.
프로그램은 업무 테이블의 PRIMARY KEY나 파티션을 자동 결정하지 않습니다.

테이블별 처리 순서는 다음과 같습니다.

1. 원천·대상 테이블과 컬럼 검증
2. 원천 건수 조회
3. 전처리 후 예상 건수 계산
4. 날짜 변환 실패와 감소율 검증
5. 모든 검증 통과 후 전체 또는 해당 날짜 구간의 대상 데이터 삭제
6. 전처리 결과 INSERT
7. 대상 건수 검증
8. 성공 또는 실패 이력 기록

Kudu의 삭제와 삽입은 전체가 하나의 트랜잭션으로 묶이지 않을 수 있습니다.
적재 중 실패하면 `FAILED` 이력과 로그를 확인하고 전체 또는 실패 날짜 범위를
재실행해야 합니다.
운영 조회가 적은 시간대에 실행하는 것을 권장합니다.

검증 설정 예시는 다음과 같습니다.

```yaml
validation:
  allow_empty: false
  max_reduction_percent: 20
```

- `allow_empty: false`: 원천이 0건이면 대상 데이터를 변경하지 않음
- `max_reduction_percent`: 중복 제거 후 감소율이 설정값을 넘으면 변경 중단

## 전처리 이력

정상 실행 모드에서는 프로그램 시작 시 아래 Kudu 테이블을 확인하고, 없으면
자동 생성합니다.

```text
ai_datawarehouse.preprocessing_history
```

PRIMARY KEY는 `(run_id, target_table)`입니다. 테이블별로 `RUNNING`, `SUCCESS`,
`FAILED` 상태와 원천/처리/대상 건수, 중복 제거 건수, 날짜 오류 건수, 적용 규칙,
노이즈 제외 건수, 처리 시간, query ID 및 오류 메시지를 기록합니다. 기존에 생성된
이력 테이블에 `noise_removed_count`가 없으면 프로그램이 `ALTER TABLE`로 한 번
추가합니다.

날짜 분할 적재의 요약 건수는 설정한 전체 날짜 범위의 합계입니다. 각 구간의
시작·성공·대상 건수는 날짜별 로그에 기록되며, 상세 변경이력은 해당 구간의
원천 행만 기록합니다.

행 단위 변경 내용은 다음 Kudu 테이블에 별도로 기록합니다.

```text
ai_datawarehouse.preprocessing_change_history
```

원천 PRIMARY KEY를 `row_key`로 저장하고, 변경 컬럼, 변경 유형, 변경 전 값과
변경 후 값을 남깁니다. 변경 유형은 `TRIM`, `DATETIME_NORMALIZED`,
`DUPLICATE_REMOVED`, `NOISE_REMOVED:규칙명`입니다. 중복 및 노이즈 제거 행은
`row_key`로 제외된 원천 행을 식별할 수 있습니다. payload 원문은 기록하지
않습니다.

`ai_datawarehouse` 데이터베이스와 업무 대상 테이블은 자동 생성하지 않습니다.

### 전처리 규칙 관리 테이블

결측·중복·노이즈 규칙을 운영에서 조회할 수 있도록 다음 SQL 파일을 제공합니다.

```text
sql/preprocessing_rule.sql
```

이 SQL은 `ai_datawarehouse.preprocessing_rule` Kudu 테이블을 생성하고 현재
`config.yaml`에 정의된 규칙을 `UPSERT`합니다. 중복 규칙은 활성 규칙으로,
YAML의 노이즈 예시 3건은 `rule_enabled=false`로 등록됩니다. 결측 규칙은 업무
기준이 확정되지 않아 임의로 입력하지 않습니다.

현재 Python 배치는 규칙을 계속 `config.yaml`에서 읽습니다. 규칙 테이블은 운영
조회와 규칙 현황 관리용이며, YAML을 변경하면 SQL의 해당 UPSERT도 함께
변경해야 합니다.

`config.yaml`에는 검수와 업무 협의에 사용할 도메인 보완/제외 샘플 5건도
포함되어 있습니다. 도메인 처리 코드는 `main.py`에 연결되어 있지만 전역 기능
스위치가 기본적으로 꺼져 있으므로 현재 DW 데이터에는 적용되지 않습니다.
SQL 규칙 테이블에는 `DOMAIN`, `rule_enabled=true`로 등록하되, 샘플 테이블
자체가 비활성이므로 `table_enabled=false`로 등록합니다.

실제 컬럼과 업무 규칙을 검증한 뒤 다음 한 줄의 주석을 풀면 도메인 보완과
제외 처리가 활성화됩니다.

```yaml
features:
  domain_rules_enabled: true
```

기능 스위치가 주석 상태이거나 `false`이면 `domain_rules`의 샘플 컬럼을
조회하지 않으며 생성되는 전처리 SQL에도 영향을 주지 않습니다. 활성화 전에는
반드시 실제 컬럼명으로 규칙을 수정하고 `--validate-only`, `--dry-run` 순서로
검증해야 합니다.

Kerberos 인증 후 Impala Shell로 실행하는 예시는 다음과 같습니다.

```bash
kinit -kt /secure/path/service_account.keytab service_account@EXAMPLE.COM
impala-shell -k \
  -i impala-coordinator.example.com:21000 \
  -f sql/preprocessing_rule.sql
```

Impala Shell 포트는 Python 연결에 사용하는 HS2 포트와 다를 수 있으므로 운영
CDP 설정을 확인해야 합니다. 실행 계정에는 `ai_datawarehouse`의 `CREATE`,
`SELECT`, `INSERT` 권한이 필요합니다.

## 로그

모든 실행은 collect 서버의 날짜별 파일에 기록합니다.

```text
logs/preprocessing_20261012.log
logs/preprocessing_20261013.log
```

같은 날짜의 수동 실행과 자동 실행은 같은 파일에 이어 쓰며, 각 실행은
`run_id`로 구분합니다. 날짜가 바뀐 후 발생한 로그는 다음 날짜 파일에 기록됩니다.
서버 시간대는 `Asia/Seoul`로 설정하는 것을 권장합니다.

로그에는 건수, 규칙, 처리 시간, 상태와 오류를 기록하지만 다음 값은 기록하지
않습니다.

- payload 및 원본 행
- 개인정보 원문
- keytab 또는 비밀번호
- Kerberos ticket 내용

운영 서버에서 `logrotate` 또는 사내 정책으로 오래된 로그를 압축·삭제하십시오.

## Python 패키지 설치

인터넷이 가능한 준비 환경에서는 일반 Python wheel을 `library_bsn`에 내려받고,
폐쇄망에서는 로컬 파일만 사용합니다.

Linux collect 서버 설치 예시:

```bash
python3.8 -m venv .venv
.venv/bin/python -m pip install --no-index \
  --find-links library_bsn/linux -r requirements.txt
chmod 750 run_batch.sh
```

Windows 개발 환경 설치 예시:

```powershell
py -3.8 -m venv .venv
.venv\Scripts\python.exe -m pip install --no-index `
  --find-links library_bsn\windows -r requirements.txt
```

wheel 파일은 Python 3.8 및 대상 운영체제/CPU와 호환되어야 합니다.

## Kerberos 운영 사전 조건

Impyla 자체의 필수 통신 의존성인 `thrift-sasl`과 `pure-sasl`은 일반 wheel로
포함되어 있습니다. 요청에 따라 실제 GSSAPI 인증에 필요한 `kerberos` Python
모듈과 Kerberos RPM, DEB, 설치 프로그램은 프로젝트에 포함하지 않습니다.
운영 환경에서 아래 항목을 확인하고 준비해야 합니다.

- `/etc/krb5.conf`와 Kerberos realm 설정
- `kinit`, `klist`, `kdestroy` 명령
- 배치 전용 principal과 keytab
- Python Kerberos/GSSAPI 모듈 및 네이티브 라이브러리
- `krb5-libs`, `krb5-workstation`, SASL 계열 OS 패키지
- Impala Kerberos service name(일반적으로 `impala`)
- Impala Coordinator FQDN과 HS2 포트
- DNS 정방향/역방향 조회와 서버 시간 동기화
- `ai_lake` SELECT 및 `ai_datawarehouse` Kudu DML 권한
- 최초 이력 테이블 생성을 위한 CREATE 권한과 기존 이력 스키마 변경을 위한
  ALTER 권한

선택한 Impyla/GSSAPI 연결 방식에 필요한 Python Kerberos 모듈은 운영 환경에서
설치해야 합니다. 인증 성공과 Ranger/Sentry 권한은 별개이므로 모두 확인합니다.

### 서버에 존재하는 keytab 연결

keytab을 프로젝트 폴더로 복사하지 않습니다. 서버에 이미 존재하는 keytab의
절대 경로만 `config.yaml`에 지정합니다. 배치 실행 계정에 읽기 권한이 있어야
하며, 다른 계정에 노출되지 않도록 운영 서버에서는 `600` 권한을 권장합니다.

```yaml
kerberos:
  enabled: true
  manage_ticket: true
  principal: ai_lake_batch@EXAMPLE.COM
  keytab: /etc/security/keytabs/ai_lake_batch.keytab
  krb5_config: /etc/krb5.conf
  cache_dir: run/krb5_cache
```

`manage_ticket: true`이면 Python이 다음 작업을 수행합니다.

1. `krb5.conf`, keytab 파일과 읽기 권한 확인
2. `kinit`, `klist`, `kdestroy` 설치 여부 확인
3. keytab에 설정한 principal이 존재하는지 확인
4. 실행 프로세스별 고유 credential cache 생성
5. keytab으로 `kinit` 수행 및 발급 principal 확인
6. 테이블 처리 전 ticket 유효성 재확인 및 만료 시 재발급
7. 종료 시 이 프로세스가 만든 cache만 삭제
8. Impala 연결 직후 `SELECT 1`로 GSSAPI 세션 확인

서버 또는 배포 시스템에서 값을 주입하려면 아래 환경변수를 사용할 수 있으며
환경변수가 YAML보다 우선합니다.

```bash
export BATCH_KRB_PRINCIPAL='ai_lake_batch@EXAMPLE.COM'
export BATCH_KEYTAB='/etc/security/keytabs/ai_lake_batch.keytab'
export KRB5_CONFIG='/etc/krb5.conf'
```

`manage_ticket: false`는 서버에서 이미 발급한 ticket을 사용해야 하는 특별한
경우에만 사용합니다. 이 경우 Python은 `kinit`이나 `kdestroy`를 실행하지 않고
현재 `KRB5CCNAME`의 ticket 유효성만 확인합니다.

## 설정

`config.yaml`의 예제 테이블은 안전을 위해 `enabled: false`입니다. 실제 테이블명,
컬럼 및 규칙을 검토한 뒤 활성화하십시오. keytab 파일 내용은 저장하지 않으며
서버에 이미 존재하는 파일의 절대 경로만 설정합니다.

TLS 또는 HTTP transport를 사용하는 환경에서는 `impala` 설정에 다음 항목을
추가할 수 있습니다.

```yaml
impala:
  use_ssl: true
  ca_cert: /secure/path/ca.pem
  use_http_transport: true
  http_path: cliservice
```

## 실행

먼저 `config.yaml`에서 Kerberos 설정과 서버의 기존 keytab 절대 경로를 확인하고
`kerberos.enabled: true`로 변경합니다. 환경변수 방식은 필요한 경우에만
사용합니다.

설정과 테이블 구조만 확인:

```bash
./run_batch.sh --validate-only
```

건수와 전처리 결과를 예상하되 대상은 변경하지 않음:

```bash
./run_batch.sh --dry-run
```

활성화된 전체 테이블 실행:

```bash
./run_batch.sh
```

특정 설정 하나만 실행:

```bash
./run_batch.sh --table customer
```

### Shell 스크립트 없이 명령어로 직접 실행

`run_batch.sh`를 사용하지 않고 명령어를 직접 입력할 수도 있습니다. Python이
YAML 설정에 따라 고유 credential cache 생성부터 ticket 정리까지 수행하므로
별도로 `kinit`, `kdestroy`를 입력할 필요가 없습니다.

```bash
cd /운영/설치경로/ai_lake_preprocessing
```

설정과 테이블 구조만 확인:

```bash
.venv/bin/python main.py --config config.yaml --validate-only
```

건수와 전처리 결과를 예상하되 대상 데이터는 변경하지 않음:

```bash
.venv/bin/python main.py --config config.yaml --dry-run
```

활성화된 전체 테이블 실행:

```bash
.venv/bin/python main.py --config config.yaml
```

특정 테이블만 실행:

```bash
.venv/bin/python main.py --config config.yaml --table customer
```

`manage_ticket: true`에서는 Python이 자신이 생성한 ticket만 정리합니다. 다른
프로세스나 공용 credential cache에는 영향을 주지 않습니다.

상세 SQL 확인이 필요한 개발 환경에서만 `--debug`를 사용합니다. 전체 SQL에
민감한 조건이 포함될 수 있으므로 운영 로그에서는 기본 INFO 레벨을 사용합니다.

## Windows 로컬 샘플 흐름 테스트

Windows PC에 Impala와 Kudu가 없어도 `local_sample_test.py`로 전체 처리 흐름을
모사할 수 있습니다. 이 테스트는 외부 패키지 없이 Python 표준 라이브러리의
SQLite를 사용하므로 운영 Kerberos 인증이 필요하지 않습니다.

기본적으로 1,000건 수집을 10회 누적하고, 매 수집 후 전처리를 실행합니다.
따라서 최종 ai_lake에는 10,000건, 전처리 이력에는 10건이 저장됩니다. 각
수집 데이터에는 다음 내용이 포함됩니다.

- 업무 키 900개
- 최신 중복 데이터 100건
- 앞뒤 공백이 포함된 문자열
- 세 가지 입력 날짜 형식
- 일부 잘못된 날짜
- 공백과 특수문자를 포함한 payload

Windows PowerShell에서 실행:

```powershell
py -3.8 local_sample_test.py
```

이력을 더 많이 만들려면 실행 횟수를 지정할 수 있습니다. 아래 명령은 30회,
총 30,000건의 누적 수집과 30건의 처리 이력을 만듭니다.

```powershell
py -3.8 local_sample_test.py --runs 30
```

`py` 명령을 사용하지 않는 환경에서는 설치된 Python 3.8 실행 파일을 직접
지정합니다.

```powershell
C:\Python38\python.exe local_sample_test.py
```

성공 시 다음 결과를 확인합니다.

- 기본 실행 기준 누적 원천 10,000건
- 최종 실행에서 중복 9,100건 제거
- DW 900건 적재
- 문자열 앞뒤 공백 제거
- 날짜를 `yyyy-MM-dd HH:mm:ss` 문자열로 통일
- 잘못된 날짜를 NULL로 처리하고 건수 기록
- payload 내용과 특수문자 원본 보존
- `preprocessing_history`에 실행 회차별 SUCCESS 10건 기록
- `preprocessing_change_history`에 행·컬럼별 변경 전후 값 기록
- 날짜별 로그 파일 생성

결과 파일은 `sample_output`에 생성됩니다.

```text
sample_output/
├─ ai_lake_multi.db
├─ ai_datawarehouse_multi.db
└─ logs/
   └─ preprocessing_YYYYMMDD.log
```

이 테스트는 로컬 처리 흐름을 검증하는 용도이며 Impala SQL 문법, Kudu DML,
Kerberos 및 Ranger/Sentry 권한까지 검증하지는 않습니다. 실제 CDP에서는 먼저
`--validate-only`와 `--dry-run`을 수행해야 합니다.

샘플 원천과 DW 데이터를 행 단위로 비교하려면 다음 명령을 실행합니다.

```powershell
py -3.8 inspect_sample.py
```

이 명령은 중복 데이터의 최신 행 선택, 문자열 공백 제거, 날짜 표준화, 잘못된
날짜의 NULL 처리, payload 원본 보존 및 전처리 이력을 함께 출력합니다.

## cron 전환

수동 실행이 안정화된 후 동일한 `run_batch.sh`를 cron에서 호출합니다. cron은
로그인 셸 환경을 상속하지 않습니다. principal과 keytab 절대 경로는
`config.yaml`에 설정하거나, 필요하면 `BATCH_KRB_PRINCIPAL`, `BATCH_KEYTAB`
환경변수로 주입합니다. `PYTHON_BIN`, `CONFIG_FILE`도 기본값과 다를 때만 별도
환경 파일 또는 운영 표준 방식으로 제공합니다. keytab 파일 내용이나 비밀번호를
crontab에 직접 기록하지 마십시오.

원천 수집 완료 이후로 실행 시간을 잡고, 먼저 수동 실행에서 다음 항목을
검증하십시오.

1. Kerberos ticket 발급
2. Impala 연결
3. 원천 SELECT 권한
4. 개발 대상 Kudu 쓰기 권한
5. dry-run 건수
6. 정상 실행과 실패 후 재실행
7. 날짜별 로그와 이력 테이블

## 종료 코드

| 코드 | 의미 |
|---:|---|
| 0 | 성공 |
| 1 | 연결 또는 SQL 등 일반 실행 오류 |
| 2 | 설정 오류 |
| 3 | `run_batch.sh`의 Kerberos 인증 오류 |
| 5 | 데이터 또는 실행 검증 실패 |
