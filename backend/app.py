from fastapi import BackgroundTasks, FastAPI, UploadFile, File, Form, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles
import csv
from collections import defaultdict, deque
import io
import json
import math
import logging
import os
import secrets
import textwrap
import tempfile
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from contextvars import ContextVar
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
from docx import Document
from dotenv import load_dotenv
from pydantic import BaseModel
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from sklearn.compose import ColumnTransformer
from sklearn.decomposition import PCA
from sklearn.ensemble import GradientBoostingClassifier, GradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    mean_absolute_error,
    mean_squared_error,
    r2_score,
)
from sklearn.model_selection import cross_validate, train_test_split
from sklearn.neighbors import KNeighborsClassifier, KNeighborsRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.inspection import permutation_importance

try:
    import xgboost as xgb
except Exception:  # noqa: BLE001
    xgb = None

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
ENV_PATH = os.path.join(BASE_DIR, ".env")
# Load project .env regardless of current working directory.
load_dotenv(ENV_PATH)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


DEBUG = os.environ.get("DEBUG", "false").lower() in ["1", "true", "yes"]
MAX_FILE_SIZE_MB = _env_int("MAX_FILE_SIZE_MB", 10)
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
MAX_ML_ROWS = _env_int("MAX_ML_ROWS", 5000)
CORS_ORIGINS = os.environ.get(
    "CORS_ORIGINS", "http://127.0.0.1:8001,http://localhost:8001"
)
OPENAI_TIMEOUT = _env_int("OPENAI_TIMEOUT", 30)
DQA_API_KEY = os.environ.get("DQA_API_KEY", "").strip()
CORS_ALLOW_CREDENTIALS = os.environ.get("CORS_ALLOW_CREDENTIALS", "false").lower() in [
    "1",
    "true",
    "yes",
]
AUTH_REQUIRED = os.environ.get("AUTH_REQUIRED", "false").lower() in ["1", "true", "yes"]
AUTH_USER = os.environ.get("AUTH_USER", "admin").strip()
AUTH_PASSWORD = os.environ.get("AUTH_PASSWORD", "").strip()
SESSION_TTL_MINUTES = max(5, _env_int("SESSION_TTL_MINUTES", 120))
ANALYZE_TIMEOUT_SEC = max(10, _env_int("ANALYZE_TIMEOUT_SEC", 120))
TRANSFORM_TIMEOUT_SEC = max(10, _env_int("TRANSFORM_TIMEOUT_SEC", 90))
TRANSFORM_RETRY_COUNT = max(0, _env_int("TRANSFORM_RETRY_COUNT", 1))
RATE_LIMIT_ANALYZE_PER_MIN = max(1, _env_int("RATE_LIMIT_ANALYZE_PER_MIN", 20))
RATE_LIMIT_AI_PER_MIN = max(1, _env_int("RATE_LIMIT_AI_PER_MIN", 30))

origins = [o.strip() for o in CORS_ORIGINS.split(",") if o.strip()]
if not origins:
    origins = ["http://127.0.0.1:8001"]
if "*" in origins and CORS_ALLOW_CREDENTIALS:
    CORS_ALLOW_CREDENTIALS = False

app = FastAPI(title="CIGMA Data Profiler")

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=CORS_ALLOW_CREDENTIALS,
    allow_methods=["*"],
    allow_headers=["*"],
)

ALLOWED_EXTENSIONS = {
    ".csv",
    ".tsv",
    ".xls",
    ".xlsx",
    ".xlxs",
    ".json",
    ".xml",
    ".docx",
}

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini").strip()

metrics_lock = Lock()
metrics = {"upload_count": 0, "total_bytes": 0}

jobs_lock = Lock()
ai_jobs: Dict[str, Dict[str, Any]] = {}

schema_cache_lock = Lock()
schema_cache: Dict[str, List[str]] = {}
MAX_SCHEMA_CACHE_ENTRIES = 500

request_id_ctx: ContextVar[str] = ContextVar("request_id", default="")

logger = logging.getLogger("cigma_dqa")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

sessions_lock = Lock()
sessions: Dict[str, Dict[str, Any]] = {}

rate_limit_lock = Lock()
rate_limit_buckets: Dict[str, deque] = defaultdict(deque)

analyze_jobs_lock = Lock()
analyze_jobs: Dict[str, Dict[str, Any]] = {}


class UploadTooLarge(Exception):
    pass


def _safe_float(value: Optional[float]) -> Optional[float]:
    return None if value is None else float(value)


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        value = float(value)
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    return value


def _current_request_id() -> str:
    return request_id_ctx.get() or ""


def _log_event(level: str, event: str, **fields: Any) -> None:
    payload = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "event": event,
        **fields,
    }
    line = json.dumps(_json_safe(payload), default=str)
    if level == "error":
        logger.error(line)
    else:
        logger.info(line)


def _extract_bearer_token(request: Request) -> str:
    auth = request.headers.get("authorization", "")
    if not auth.lower().startswith("bearer "):
        return ""
    return auth.split(" ", 1)[1].strip()


def _cleanup_sessions() -> None:
    now = time.time()
    with sessions_lock:
        expired = [token for token, info in sessions.items() if info.get("expires_at", 0) <= now]
        for token in expired:
            sessions.pop(token, None)


def _create_session(username: str) -> Dict[str, Any]:
    token = secrets.token_urlsafe(32)
    expires_at = time.time() + (SESSION_TTL_MINUTES * 60)
    session = {"token": token, "username": username, "expires_at": expires_at}
    with sessions_lock:
        sessions[token] = session
    return session


def _validate_session(token: str) -> Optional[Dict[str, Any]]:
    if not token:
        return None
    _cleanup_sessions()
    with sessions_lock:
        session = sessions.get(token)
    if not session:
        return None
    if session.get("expires_at", 0) <= time.time():
        with sessions_lock:
            sessions.pop(token, None)
        return None
    return session


def _revoke_session(token: str) -> None:
    if not token:
        return
    with sessions_lock:
        sessions.pop(token, None)


def _rate_limit_scope(path: str) -> Optional[Tuple[str, int]]:
    if path in {"/api/analyze", "/api/analyze/async"}:
        return ("analyze", RATE_LIMIT_ANALYZE_PER_MIN)
    if path.startswith("/api/ai-insights") or path == "/api/chat-assistant":
        return ("ai", RATE_LIMIT_AI_PER_MIN)
    return None


def _rate_limit_key(request: Request, session: Optional[Dict[str, Any]]) -> str:
    if session:
        return f"user:{session.get('username', 'unknown')}"
    return f"ip:{request.client.host if request.client else 'unknown'}"


def _check_rate_limit(scope: str, key: str, limit: int, window_seconds: int = 60) -> Tuple[bool, int]:
    now = time.time()
    bucket_key = f"{scope}:{key}"
    with rate_limit_lock:
        bucket = rate_limit_buckets[bucket_key]
        while bucket and (now - bucket[0]) > window_seconds:
            bucket.popleft()
        if len(bucket) >= limit:
            retry_after = max(1, int(window_seconds - (now - bucket[0])))
            return False, retry_after
        bucket.append(now)
    return True, 0


def _execute_with_timeout(
    fn: Any,
    *args: Any,
    timeout_sec: int,
    retries: int = 0,
    **kwargs: Any,
) -> Any:
    attempts = max(0, retries) + 1
    last_error: Optional[Exception] = None
    for attempt in range(attempts):
        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(fn, *args, **kwargs)
        try:
            result = future.result(timeout=max(1, timeout_sec))
            executor.shutdown(wait=False, cancel_futures=True)
            return result
        except FuturesTimeoutError:
            last_error = TimeoutError(f"Operation timed out after {timeout_sec} seconds")
            future.cancel()
            executor.shutdown(wait=False, cancel_futures=True)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            executor.shutdown(wait=False, cancel_futures=True)
        if attempt < attempts - 1:
            time.sleep(0.2)
    if last_error:
        raise last_error
    raise RuntimeError("Operation failed")


def _remove_temp_file(path: Optional[str]) -> None:
    if not path:
        return
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:  # noqa: BLE001
        pass


def _read_file_bytes(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()


async def _save_upload_to_tempfile(file: UploadFile, max_bytes: int, suffix: str = "") -> Tuple[str, int]:
    chunk_size = 1024 * 1024
    total = 0
    fd, path = tempfile.mkstemp(prefix="dqa_upload_", suffix=suffix or ".tmp")
    try:
        with os.fdopen(fd, "wb") as tmp:
            while True:
                chunk = await file.read(chunk_size)
                if not chunk:
                    break
                total += len(chunk)
                if total > max_bytes:
                    raise UploadTooLarge()
                tmp.write(chunk)
    except Exception:  # noqa: BLE001
        _remove_temp_file(path)
        raise
    return path, total


SENSITIVE_KEY_HINTS = (
    "name",
    "email",
    "phone",
    "mobile",
    "ssn",
    "address",
    "dob",
    "birth",
    "token",
    "password",
    "secret",
    "key",
)


def _neutralize_csv_cell(value: Any) -> str:
    text = str(value).replace("\r", " ").replace("\n", " ")
    if text.startswith(("=", "+", "-", "@")):
        return "'" + text
    return text



def _redact_sensitive_keys(payload: Any) -> Any:
    if isinstance(payload, dict):
        redacted: Dict[str, Any] = {}
        for key, value in payload.items():
            key_str = str(key)
            lowered = key_str.lower()
            if any(hint in lowered for hint in SENSITIVE_KEY_HINTS):
                redacted[key_str] = "[REDACTED]"
            else:
                redacted[key_str] = _redact_sensitive_keys(value)
        return redacted
    if isinstance(payload, list):
        return [_redact_sensitive_keys(v) for v in payload]
    return payload


def _error_response(code: str, message: str, status_code: int = 400, details: Any = None):
    payload: Dict[str, Any] = {
        "error": {
            "code": code,
            "message": message,
            "request_id": _current_request_id(),
        }
    }
    if details is not None:
        payload["error"]["details"] = details
    return JSONResponse(status_code=status_code, content=_json_safe(payload))


@app.middleware("http")
async def api_key_guard(request: Request, call_next):
    request_id = request.headers.get("x-request-id", "").strip() or str(uuid.uuid4())
    ctx_token = request_id_ctx.set(request_id)
    start = time.perf_counter()
    path = request.url.path
    method = request.method
    client_ip = request.client.host if request.client else "unknown"

    exempt_paths = {
        "/api/metrics",
        "/api/session/login",
        "/api/session/refresh",
        "/api/session/logout",
        "/api/session/me",
    }
    protected_api = path.startswith("/api/") and path not in exempt_paths

    try:
        session_token = _extract_bearer_token(request)
        session = _validate_session(session_token)

        api_key_valid = False
        if DQA_API_KEY:
            provided = request.headers.get("x-api-key", "")
            api_key_valid = provided == DQA_API_KEY

        if protected_api:
            if AUTH_REQUIRED:
                if not api_key_valid and not session:
                    response = _error_response("UNAUTHORIZED", "Authentication required.", 401)
                    response.headers["x-request-id"] = request_id
                    return response
            elif DQA_API_KEY and not api_key_valid:
                response = _error_response("UNAUTHORIZED", "Missing or invalid API key.", 401)
                response.headers["x-request-id"] = request_id
                return response

            rate_scope = _rate_limit_scope(path)
            if rate_scope:
                scope, limit = rate_scope
                key = _rate_limit_key(request, session)
                allowed, retry_after = _check_rate_limit(scope, key, limit)
                if not allowed:
                    response = _error_response(
                        "RATE_LIMITED",
                        "Rate limit exceeded. Please retry shortly.",
                        429,
                        {
                            "scope": scope,
                            "limit_per_minute": limit,
                            "retry_after_seconds": retry_after,
                        },
                    )
                    response.headers["x-request-id"] = request_id
                    response.headers["retry-after"] = str(retry_after)
                    return response

        response = await call_next(request)
        response.headers["x-request-id"] = request_id
        duration_ms = round((time.perf_counter() - start) * 1000, 2)
        _log_event(
            "info",
            "http_request",
            request_id=request_id,
            method=method,
            path=path,
            status_code=response.status_code,
            duration_ms=duration_ms,
            client_ip=client_ip,
        )
        return response
    except Exception as exc:  # noqa: BLE001
        duration_ms = round((time.perf_counter() - start) * 1000, 2)
        _log_event(
            "error",
            "http_exception",
            request_id=request_id,
            method=method,
            path=path,
            duration_ms=duration_ms,
            client_ip=client_ip,
            error=str(exc),
        )
        raise
    finally:
        request_id_ctx.reset(ctx_token)


def _schema_cache_key_from_values(session_id: str, host: str, file_name: str) -> str:
    sid = (session_id or "").strip()
    if sid:
        return sid[:128]
    return f"{host}:{file_name.lower()}"


def _schema_cache_key(request: Request, file_name: str) -> str:
    session_id = request.headers.get("x-session-id", "")
    host = request.client.host if request.client else "unknown"
    return _schema_cache_key_from_values(session_id, host, file_name)


def _get_previous_schema(cache_key: str) -> List[str]:
    with schema_cache_lock:
        return list(schema_cache.get(cache_key, []))


def _set_previous_schema(cache_key: str, columns: List[str]) -> None:
    with schema_cache_lock:
        if len(schema_cache) >= MAX_SCHEMA_CACHE_ENTRIES and cache_key not in schema_cache:
            first_key = next(iter(schema_cache.keys()))
            schema_cache.pop(first_key, None)
        schema_cache[cache_key] = columns


def _record_upload(size_bytes: int) -> None:
    with metrics_lock:
        metrics["upload_count"] += 1
        metrics["total_bytes"] += size_bytes


async def _read_upload_with_limit(file: UploadFile, max_bytes: int) -> bytes:
    chunk_size = 1024 * 1024
    total = 0
    buffer = bytearray()
    while True:
        chunk = await file.read(chunk_size)
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise UploadTooLarge()
        buffer.extend(chunk)
    return bytes(buffer)


def _sample_df(df: pd.DataFrame, max_rows: int) -> pd.DataFrame:
    if len(df) <= max_rows:
        return df
    return df.sample(n=max_rows, random_state=42)


def _determine_problem_type(series: pd.Series) -> str:
    if pd.api.types.is_numeric_dtype(series):
        unique_count = series.nunique(dropna=True)
        ratio = unique_count / max(len(series), 1)
        if unique_count <= 20 and ratio <= 0.05:
            return "classification"
        return "regression"
    return "classification"


def _build_preprocessor(X: pd.DataFrame) -> ColumnTransformer:
    numeric_features = X.select_dtypes(include="number").columns.tolist()
    categorical_features = X.select_dtypes(exclude="number").columns.tolist()

    numeric_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )
    categorical_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore")),
        ]
    )

    return ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, numeric_features),
            ("cat", categorical_transformer, categorical_features),
        ],
        remainder="drop",
    )


def _light_eda(df: pd.DataFrame, target_column: str) -> Dict[str, Any]:
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    correlations = []
    if len(numeric_cols) >= 2:
        corr = df[numeric_cols].corr().abs()
        mask = ~np.eye(len(corr), dtype=bool)
        pairs = corr.where(mask).stack().sort_values(ascending=False)
        for (c1, c2), value in pairs.head(5).items():
            correlations.append({"col1": c1, "col2": c2, "corr": round(float(value), 4)})

    target_balance = {}
    if target_column and target_column in df.columns:
        target_balance = (
            df[target_column].astype(str).value_counts().head(20).to_dict()
        )

    return {
        "rows": int(len(df)),
        "columns": int(df.shape[1]),
        "numeric_columns": numeric_cols[:20],
        "correlations": correlations,
        "target_balance": target_balance,
    }


def _compute_pca(df: pd.DataFrame) -> Dict[str, Any]:
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    if len(numeric_cols) < 2:
        return {"status": "skipped", "reason": "Not enough numeric columns"}

    X = df[numeric_cols].fillna(df[numeric_cols].median())
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    pca = PCA(n_components=min(5, X_scaled.shape[1]))
    components = pca.fit_transform(X_scaled)
    explained = pca.explained_variance_ratio_.tolist()
    return {
        "status": "ok",
        "explained_variance_ratio": [round(float(v), 4) for v in explained],
        "components_preview": components[:5].tolist(),
    }


def _time_series_baselines(df: pd.DataFrame, time_column: str, target_column: str) -> Dict[str, Any]:
    if time_column not in df.columns or target_column not in df.columns:
        return {"status": "skipped", "reason": "Missing time or target column"}
    try:
        ts = df[[time_column, target_column]].dropna()
        ts[time_column] = pd.to_datetime(ts[time_column])
        ts = ts.sort_values(time_column)
        if len(ts) < 10:
            return {"status": "skipped", "reason": "Not enough rows for time series"}
        values = ts[target_column]
        split_idx = int(len(values) * 0.8)
        train = values.iloc[:split_idx]
        test = values.iloc[split_idx:]
        last_value = train.iloc[-1]
        naive_pred = pd.Series([last_value] * len(test), index=test.index)
        rolling_mean = train.rolling(window=min(5, len(train))).mean().iloc[-1]
        rolling_pred = pd.Series([rolling_mean] * len(test), index=test.index)

        return {
            "status": "ok",
            "naive": {
                "mae": round(float(mean_absolute_error(test, naive_pred)), 4),
                "rmse": round(float(mean_squared_error(test, naive_pred, squared=False)), 4),
            },
            "rolling_mean": {
                "mae": round(float(mean_absolute_error(test, rolling_pred)), 4),
                "rmse": round(float(mean_squared_error(test, rolling_pred, squared=False)), 4),
            },
        }
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "reason": str(exc)}


def _resolve_models(problem_type: str, model_names: List[str]) -> List[Tuple[str, Any]]:
    names = [name.lower() for name in model_names if name]
    models: List[Tuple[str, Any]] = []

    def include(key: str) -> bool:
        return not names or key in names

    if problem_type == "classification":
        if include("logistic"):
            models.append(("Logistic Regression", LogisticRegression(max_iter=1000)))
        if include("knn"):
            models.append(("KNN Classifier", KNeighborsClassifier()))
        if include("gradient_boost"):
            models.append(("Gradient Boosting", GradientBoostingClassifier()))
        if include("xgboost") and xgb is not None:
            models.append(("XGBoost", xgb.XGBClassifier(eval_metric="logloss")))
    else:
        if include("linear"):
            models.append(("Linear Regression", LinearRegression()))
        if include("knn"):
            models.append(("KNN Regressor", KNeighborsRegressor()))
        if include("gradient_boost"):
            models.append(("Gradient Boosting", GradientBoostingRegressor()))
        if include("xgboost") and xgb is not None:
            models.append(("XGBoost", xgb.XGBRegressor()))

    return models


def _safe_cv(df: pd.DataFrame, cv_folds: int) -> int:
    if cv_folds < 2:
        return 0
    return min(cv_folds, max(2, len(df) // 5))


def _train_models(
    df: pd.DataFrame,
    target_column: str,
    model_names: Optional[List[str]] = None,
    test_size: float = 0.2,
    cv_folds: int = 3,
) -> Dict[str, Any]:
    if not target_column or target_column not in df.columns:
        return {"status": "skipped", "reason": "Target column not provided"}

    df = df.dropna(subset=[target_column])
    if len(df) < 20:
        return {"status": "skipped", "reason": "Not enough rows for training"}

    y = df[target_column]
    X = df.drop(columns=[target_column])
    problem_type = _determine_problem_type(y)
    if problem_type == "classification" and y.nunique() < 2:
        return {"status": "skipped", "reason": "Target has only one class"}
    preprocessor = _build_preprocessor(X)

    stratify = y if problem_type == "classification" and y.nunique() > 1 else None
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=42, stratify=stratify
    )

    results: Dict[str, Any] = {
        "problem_type": problem_type,
        "models": [],
        "cv_folds": _safe_cv(df, cv_folds),
        "test_size": test_size,
    }

    selected = _resolve_models(problem_type, model_names or [])
    for name, model in selected:
        pipeline = Pipeline([("pre", preprocessor), ("model", model)])
        pipeline.fit(X_train, y_train)
        preds = pipeline.predict(X_test)

        model_result: Dict[str, Any] = {"name": name}
        if problem_type == "classification":
            model_result.update(
                {
                    "accuracy": round(float(accuracy_score(y_test, preds)), 4),
                    "f1_macro": round(float(f1_score(y_test, preds, average="macro")), 4),
                }
            )
        else:
            model_result.update(
                {
                    "mae": round(float(mean_absolute_error(y_test, preds)), 4),
                    "rmse": round(float(mean_squared_error(y_test, preds, squared=False)), 4),
                    "r2": round(float(r2_score(y_test, preds)), 4),
                }
            )

        if results["cv_folds"] >= 2:
            scoring = (
                {"accuracy": "accuracy", "f1": "f1_macro"}
                if problem_type == "classification"
                else {
                    "r2": "r2",
                    "mae": "neg_mean_absolute_error",
                    "rmse": "neg_root_mean_squared_error",
                }
            )
            cv_scores = cross_validate(
                pipeline,
                X,
                y,
                cv=results["cv_folds"],
                scoring=scoring,
                n_jobs=None,
            )
            if problem_type == "classification":
                model_result["cv_accuracy"] = round(float(cv_scores["test_accuracy"].mean()), 4)
                model_result["cv_f1_macro"] = round(float(cv_scores["test_f1"].mean()), 4)
            else:
                model_result["cv_r2"] = round(float(cv_scores["test_r2"].mean()), 4)
                model_result["cv_mae"] = round(float(-cv_scores["test_mae"].mean()), 4)
                model_result["cv_rmse"] = round(float(-cv_scores["test_rmse"].mean()), 4)

        try:
            feature_names = pipeline.named_steps["pre"].get_feature_names_out()
            perm = permutation_importance(
                pipeline, X_test, y_test, n_repeats=5, random_state=42
            )
            importances = perm.importances_mean
            top_idx = importances.argsort()[::-1][:10]
            model_result["feature_importance"] = [
                {
                    "feature": str(feature_names[i]),
                    "importance": round(float(importances[i]), 4),
                }
                for i in top_idx
            ]
        except Exception:
            model_result["feature_importance"] = []

        results["models"].append(model_result)

    if xgb is None and (not model_names or "xgboost" in (model_names or [])):
        results["xgboost"] = "not_installed"

    return results


def _nature_from_ext(ext: str) -> str:
    return {
        ".csv": "csv",
        ".tsv": "tsv",
        ".xls": "xlsx",
        ".xlsx": "xlsx",
        ".xlxs": "xlsx",
        ".json": "json",
        ".xml": "xml",
        ".docx": "docx",
    }.get(ext.lower(), "unknown")


def _detect_delimiter(sample_text: str, fallback: str = ",") -> str:
    try:
        dialect = csv.Sniffer().sniff(sample_text, delimiters=[",", ";", "\t", "|"])
        return dialect.delimiter
    except Exception:
        return fallback


def _clean_text_bytes(data: bytes) -> bytes:
    # Some CSV exports include NUL bytes that break pandas parsing.
    return data.replace(b"\x00", b"")


def _looks_like_delimited_text(data: bytes) -> bool:
    sample = _clean_text_bytes(data[:65536])
    try:
        text_sample = sample.decode("utf-8", errors="ignore")
    except Exception:
        return False
    if "\n" not in text_sample:
        return False
    delimiters = [",", ";", "\t", "|"]
    return sum(text_sample.count(d) for d in delimiters) >= 3


def _read_delimited_with_fallback(data: bytes, forced_sep: Optional[str] = None) -> pd.DataFrame:
    data = _clean_text_bytes(data)
    encodings = ["utf-8-sig", "utf-8", "cp1252", "latin-1", "utf-16"]
    last_error: Optional[Exception] = None

    for encoding in encodings:
        if forced_sep:
            sep_candidates: List[Optional[str]] = [forced_sep]
        else:
            try:
                sample_text = data[:65536].decode(encoding)
                detected = _detect_delimiter(sample_text)
                sep_candidates = [None, detected, ",", ";", "\t", "|"]
            except UnicodeDecodeError as exc:
                last_error = exc
                continue

        for sep in sep_candidates:
            kwargs: Dict[str, Any] = {"encoding": encoding}
            if sep is None:
                kwargs.update({"sep": None, "engine": "python"})
            else:
                kwargs["sep"] = sep
            try:
                return pd.read_csv(io.BytesIO(data), **kwargs)
            except Exception as exc:
                last_error = exc

        try:
            tolerant_kwargs: Dict[str, Any] = {
                "encoding": encoding,
                "engine": "python",
                "on_bad_lines": "skip",
            }
            if forced_sep:
                tolerant_kwargs["sep"] = forced_sep
            else:
                tolerant_kwargs["sep"] = None
            df = pd.read_csv(io.BytesIO(data), **tolerant_kwargs)
            if not df.empty:
                return df
        except Exception as exc:
            last_error = exc

    raise ValueError(
        "Unable to parse delimited file. Ensure valid CSV/TSV encoding and consistent delimiters."
    ) from last_error


def _read_excel_with_fallback(ext: str, data: bytes) -> pd.DataFrame:
    attempted: List[str] = []

    if ext in [".xlsx", ".xlxs"]:
        try:
            return pd.read_excel(io.BytesIO(data), engine="openpyxl")
        except Exception as exc:
            attempted.append(f"openpyxl: {exc}")

    if ext == ".xls":
        try:
            return pd.read_excel(io.BytesIO(data), engine="xlrd")
        except ImportError:
            attempted.append("xlrd not installed for .xls support")
        except Exception as exc:
            attempted.append(f"xlrd: {exc}")

    try:
        return pd.read_excel(io.BytesIO(data))
    except Exception as exc:
        attempted.append(f"default excel reader: {exc}")

    # Some files are mislabeled as Excel but actually CSV/TSV text.
    if _looks_like_delimited_text(data):
        try:
            return _read_delimited_with_fallback(data)
        except Exception as exc:
            attempted.append(f"text-delimited fallback: {exc}")

    raise ValueError(
        "Unable to parse spreadsheet. "
        + " | ".join(attempted[:3])
        + (" | Install xlrd for legacy .xls files." if ext == ".xls" else "")
    )


def _read_tabular(ext: str, data: bytes) -> pd.DataFrame:
    ext = ext.lower().strip()
    if ext == ".csv":
        return _read_delimited_with_fallback(data)
    if ext == ".tsv":
        return _read_delimited_with_fallback(data, forced_sep="	")
    if ext in [".xls", ".xlsx", ".xlxs"]:
        return _read_excel_with_fallback(ext, data)
    if ext == ".json":
        try:
            return pd.read_json(io.BytesIO(data))
        except ValueError:
            payload = json.loads(data.decode("utf-8"))
            if isinstance(payload, list):
                return pd.json_normalize(payload)
            if isinstance(payload, dict):
                return pd.json_normalize(payload)
            raise
    if ext == ".xml":
        return pd.read_xml(io.BytesIO(data))
    raise ValueError("Unsupported tabular format")



def _infer_column_type(series: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(series):
        return "boolean"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "datetime"
    if pd.api.types.is_numeric_dtype(series):
        return "numeric"
    return "categorical"


def _histogram(series: pd.Series, bins: int = 10) -> List[Dict[str, Any]]:
    values = series.dropna()
    if values.empty:
        return []
    try:
        binned = pd.cut(values, bins=bins)
        counts = binned.value_counts().sort_index()
        output = []
        for interval, count in counts.items():
            output.append(
                {
                    "min": _safe_float(interval.left),
                    "max": _safe_float(interval.right),
                    "count": int(count),
                }
            )
        return output
    except Exception:
        return []


def _distribution_label(series: pd.Series) -> str:
    values = series.dropna()
    if len(values) < 8:
        return "insufficient data"
    skew = _safe_float(values.skew())
    kurt = _safe_float(values.kurtosis())
    if skew is None or kurt is None:
        return "unknown"
    if abs(skew) < 0.5 and -1 <= kurt <= 1:
        return "approximately normal"
    if skew >= 0.5:
        return "right-skewed"
    if skew <= -0.5:
        return "left-skewed"
    return "non-normal"


def _outlier_summary(series: pd.Series) -> Dict[str, Any]:
    clean = series.dropna()
    if len(clean) < 4:
        return {"iqr": 0, "zscore": 0, "mad": 0}

    q1 = clean.quantile(0.25)
    q3 = clean.quantile(0.75)
    iqr = q3 - q1
    if iqr > 0:
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        iqr_outliers = int(((clean < lower) | (clean > upper)).sum())
    else:
        iqr_outliers = 0

    mean = clean.mean()
    std = clean.std(ddof=1)
    if std and std > 0:
        zscores = (clean - mean).abs() / std
        z_outliers = int((zscores > 3).sum())
    else:
        z_outliers = 0

    median = clean.median()
    mad = (clean - median).abs().median()
    if mad and mad > 0:
        modified_z = 0.6745 * (clean - median).abs() / mad
        mad_outliers = int((modified_z > 3.5).sum())
    else:
        mad_outliers = 0

    return {"iqr": iqr_outliers, "zscore": z_outliers, "mad": mad_outliers}


def _compute_quality_score(
    df: pd.DataFrame,
    missing_values: Dict[str, int],
    duplicates: int,
    outlier_summary: Dict[str, Any],
    schema_drift: Dict[str, Any],
) -> Dict[str, Any]:
    total_cells = max(df.shape[0] * df.shape[1], 1)
    missing_total = sum(missing_values.values())
    missing_rate = missing_total / total_cells
    dup_rate = duplicates / max(len(df), 1)

    outlier_counts = 0
    numeric_cols = len(outlier_summary)
    for stats in outlier_summary.values():
        outlier_counts += max(stats.get("iqr", 0), stats.get("zscore", 0), stats.get("mad", 0))
    outlier_rate = outlier_counts / max(len(df) * max(numeric_cols, 1), 1)

    drift_rate = schema_drift.get("drift_rate", 0.0)

    weights = {"missing": 0.35, "duplicates": 0.2, "outliers": 0.25, "schema_drift": 0.2}
    penalty = (
        weights["missing"] * min(missing_rate / 0.2, 1.0)
        + weights["duplicates"] * min(dup_rate / 0.1, 1.0)
        + weights["outliers"] * min(outlier_rate / 0.05, 1.0)
        + weights["schema_drift"] * min(drift_rate / 0.3, 1.0)
    )
    score = max(0, 100 - int(penalty * 100))

    return {
        "score": score,
        "weights": weights,
        "missing_rate": round(missing_rate, 4),
        "duplicate_rate": round(dup_rate, 4),
        "outlier_rate": round(outlier_rate, 4),
        "schema_drift_rate": round(drift_rate, 4),
    }


def _schema_drift(df: pd.DataFrame, previous_columns: Optional[List[str]]) -> Dict[str, Any]:
    if not previous_columns:
        return {"status": "not_available", "drift_rate": 0.0}
    current = set(df.columns.astype(str))
    prev = set(previous_columns)
    added = sorted(list(current - prev))
    removed = sorted(list(prev - current))
    total = max(len(prev), 1)
    drift_rate = (len(added) + len(removed)) / total
    return {
        "status": "ok",
        "added": added,
        "removed": removed,
        "drift_rate": drift_rate,
    }


def _root_cause_and_blast_radius(
    quality: Dict[str, Any],
    missing_values: Dict[str, int],
    duplicates: int,
    outlier_summary: Dict[str, Any],
    label_distribution: Dict[str, Any],
) -> Dict[str, Any]:
    severity = "low"
    if quality["score"] < 70:
        severity = "high"
    elif quality["score"] < 85:
        severity = "medium"

    top_missing = sorted(missing_values.items(), key=lambda x: x[1], reverse=True)[:3]
    outlier_cols = [
        col for col, stats in outlier_summary.items()
        if max(stats.get("iqr", 0), stats.get("zscore", 0), stats.get("mad", 0)) > 0
    ][:3]

    impacted_kpis = []
    if label_distribution:
        impacted_kpis.append("Target label balance / model fairness")
    if duplicates > 0:
        impacted_kpis.append("Unique customer counts / churn rates")
    if top_missing:
        impacted_kpis.append("Revenue / conversion metrics")
    if outlier_cols:
        impacted_kpis.append("Forecast accuracy / anomaly alerts")

    root_causes = []
    if top_missing:
        root_causes.append("Upstream nulls from ingestion or joins")
    if duplicates > 0:
        root_causes.append("Lack of deduplication on source keys")
    if outlier_cols:
        root_causes.append("Sensor or measurement errors / unit mismatch")

    return {
        "risk_severity": severity,
        "top_missing_columns": top_missing,
        "outlier_columns": outlier_cols,
        "impacted_kpis": impacted_kpis,
        "root_cause_hypotheses": root_causes,
    }


def _auto_remediation_plan(
    missing_values: Dict[str, int],
    duplicates: int,
    outlier_summary: Dict[str, Any],
) -> Dict[str, Any]:
    steps = []
    if duplicates > 0:
        steps.append("Remove duplicate rows based on primary keys")
    if sum(missing_values.values()) > 0:
        steps.append("Impute missing values (median for numeric, mode for categorical)")
        steps.append("Audit upstream sources causing nulls")
    if any(
        max(stats.get("iqr", 0), stats.get("zscore", 0), stats.get("mad", 0)) > 0
        for stats in outlier_summary.values()
    ):
        steps.append("Winsorize or cap outliers using IQR or robust z-score")
        steps.append("Investigate outlier sources for process errors")

    if not steps:
        steps.append("No remediation needed. Data quality is strong.")

    return {
        "steps": steps,
        "one_click_actions": ["Remove duplicates", "Impute missing values", "Cap outliers"],
    }


def _truncate_dict(data: Dict[str, Any], limit: int = 50) -> Dict[str, Any]:
    if len(data) <= limit:
        return data
    return dict(list(data.items())[:limit])


def _build_ai_summary(analysis: Dict[str, Any]) -> Dict[str, Any]:
    summary = {
        "file_name": analysis.get("file_name"),
        "nature": analysis.get("nature"),
        "data_type": analysis.get("data_type"),
        "rows": analysis.get("rows"),
        "columns": analysis.get("columns"),
        "grain": analysis.get("grain"),
        "analysis_intent": analysis.get("analysis_intent"),
        "target_column": analysis.get("target_column"),
        "analysis_fit": analysis.get("analysis_fit"),
        "suggestions": analysis.get("suggestions", []),
        "missing_values": _redact_sensitive_keys(_truncate_dict(analysis.get("missing_values", {}))),
        "column_profiles": _redact_sensitive_keys(_truncate_dict(analysis.get("column_profiles", {}))),
        "numeric_distributions": _redact_sensitive_keys(_truncate_dict(analysis.get("numeric_distributions", {}))),
        "distribution_labels": _redact_sensitive_keys(_truncate_dict(analysis.get("distribution_labels", {}))),
        "outlier_summary": _redact_sensitive_keys(_truncate_dict(analysis.get("outlier_summary", {}))),
        "label_distribution": _redact_sensitive_keys(_truncate_dict(analysis.get("label_distribution", {}))),
    }
    return summary


def _extract_output_text(payload: Dict[str, Any]) -> str:
    output_text = []
    for item in payload.get("output", []):
        for content in item.get("content", []):
            if content.get("type") == "output_text":
                output_text.append(content.get("text", ""))
    if output_text:
        return "".join(output_text).strip()
    return payload.get("output_text", "").strip()


def _generate_ai_insights(analysis: Dict[str, Any]) -> Dict[str, Any]:
    summary = _build_ai_summary(analysis)
    system_prompt = textwrap.dedent(
        """
        You are a data quality and analytics advisor.
        Provide concise, actionable guidance based only on the provided summary.
        """
    ).strip()

    user_prompt = textwrap.dedent(
        f"""
        Dataset summary (no raw data):
        {json.dumps(summary, indent=2)}

        Return:
        1) Executive summary (3-5 bullets)
        2) Data quality risks (bullets)
        3) Potential blast radius / business impact (bullets)
        4) Root cause hypotheses (bullets)
        5) Recommended remediation steps (ordered list)
        6) Suggested analyses (bullets)
        7) Suggested ML models (bullets)
        """
    ).strip()

    payload_body = {
        "model": OPENAI_MODEL,
        "input": [
            {
                "role": "system",
                "content": [{"type": "input_text", "text": system_prompt}],
            },
            {
                "role": "user",
                "content": [{"type": "input_text", "text": user_prompt}],
            },
        ],
        "temperature": 0.2,
    }

    response = requests.post(
        "https://api.openai.com/v1/responses",
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        },
        json=payload_body,
        timeout=OPENAI_TIMEOUT,
    )
    if response.status_code >= 400:
        raise RuntimeError(response.text)

    data = response.json()
    ai_text = _extract_output_text(data)
    if not ai_text:
        ai_text = "No AI output returned."

    return {
        "model": OPENAI_MODEL,
        "summary": summary,
        "ai_text": ai_text,
        "request_id": response.headers.get("x-request-id", ""),
    }


def _run_ai_job(job_id: str, analysis: Dict[str, Any]) -> None:
    with jobs_lock:
        ai_jobs[job_id] = {"status": "running", "started_at": time.time()}
    try:
        result = _generate_ai_insights(analysis)
        with jobs_lock:
            ai_jobs[job_id] = {
                "status": "done",
                "result": result,
                "completed_at": time.time(),
            }
    except Exception as exc:  # noqa: BLE001
        with jobs_lock:
            ai_jobs[job_id] = {
                "status": "error",
                "error": str(exc),
                "completed_at": time.time(),
            }


def _column_anomalies(series: pd.Series, column_type: str) -> List[str]:
    anomalies = []
    total = len(series)
    if total == 0:
        return anomalies

    missing_rate = series.isna().mean()
    if missing_rate >= 0.2:
        anomalies.append("High missing rate")

    unique_count = series.nunique(dropna=True)
    if unique_count <= 1:
        anomalies.append("Constant values")

    if column_type in ["categorical", "boolean"] and unique_count / max(total, 1) > 0.9:
        anomalies.append("High cardinality")

    if column_type == "numeric":
        clean = series.dropna()
        if len(clean) >= 4:
            q1 = clean.quantile(0.25)
            q3 = clean.quantile(0.75)
            iqr = q3 - q1
            if iqr > 0:
                lower = q1 - 1.5 * iqr
                upper = q3 + 1.5 * iqr
                outlier_rate = ((clean < lower) | (clean > upper)).mean()
                if outlier_rate >= 0.05:
                    anomalies.append("Outliers detected")

    return anomalies


def _numeric_correlation_matrix(df: pd.DataFrame, numeric_cols: List[str]) -> Dict[str, Dict[str, Optional[float]]]:
    if len(numeric_cols) < 2:
        return {}

    limited_cols = numeric_cols[:12]
    corr = df[limited_cols].corr(numeric_only=True)
    result: Dict[str, Dict[str, Optional[float]]] = {}
    for row_col in limited_cols:
        row_values: Dict[str, Optional[float]] = {}
        for col in limited_cols:
            value = corr.loc[row_col, col]
            if pd.isna(value):
                row_values[col] = None
            else:
                row_values[col] = round(float(value), 4)
        result[row_col] = row_values
    return result


def _analyze_dataframe(
    df: pd.DataFrame, target_column: str = "", previous_columns: Optional[List[str]] = None
) -> Dict[str, Any]:
    rows, cols = df.shape
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    categorical_cols = df.select_dtypes(exclude="number").columns.tolist()

    data_type = "quantitative" if len(numeric_cols) > 0 else "qualitative"

    missing = df.isna().sum().to_dict()
    missing = {str(k): int(v) for k, v in missing.items()}

    distributions: Dict[str, Any] = {}
    histograms: Dict[str, Any] = {}
    distribution_labels: Dict[str, Any] = {}
    outliers: Dict[str, Any] = {}
    for col in numeric_cols:
        series = df[col].dropna()
        if series.empty:
            distributions[col] = {
                "count": 0,
                "mean": None,
                "std": None,
                "min": None,
                "p25": None,
                "median": None,
                "p75": None,
                "max": None,
            }
            histograms[col] = []
            distribution_labels[col] = "insufficient data"
            outliers[col] = {"iqr": 0, "zscore": 0, "mad": 0}
            continue
        distributions[col] = {
            "count": int(series.count()),
            "mean": _safe_float(series.mean()),
            "std": _safe_float(series.std(ddof=1)) if series.count() > 1 else None,
            "min": _safe_float(series.min()),
            "p25": _safe_float(series.quantile(0.25)),
            "median": _safe_float(series.median()),
            "p75": _safe_float(series.quantile(0.75)),
            "max": _safe_float(series.max()),
        }
        histograms[col] = _histogram(series)
        distribution_labels[col] = _distribution_label(series)
        outliers[col] = _outlier_summary(series)

    categorical_summaries: Dict[str, Any] = {}
    for col in categorical_cols:
        series = df[col].dropna().astype(str)
        top = series.value_counts().head(5).to_dict()
        categorical_summaries[col] = {str(k): int(v) for k, v in top.items()}

    column_profiles: Dict[str, Any] = {}
    for col in df.columns:
        series = df[col]
        col_type = _infer_column_type(series)
        column_profiles[str(col)] = {
            "type": col_type,
            "missing": int(series.isna().sum()),
            "missing_rate": _safe_float(series.isna().mean()),
            "unique": int(series.nunique(dropna=True)),
            "anomalies": _column_anomalies(series, col_type),
        }

    suggestions = [
        "Row/column profiling",
        "Missing value patterns",
        "Anomaly flags and type inference",
    ]
    if numeric_cols:
        suggestions.extend(
            [
                "Descriptive statistics",
                "Distributions and outliers",
                "Correlations between numeric columns",
            ]
        )
    if categorical_cols:
        suggestions.extend(
            [
                "Value counts and mode analysis",
                "Category balance checks",
            ]
        )

    label_distribution = {}
    target_column = target_column.strip()
    if target_column and target_column in df.columns:
        target_series = df[target_column].dropna()
        label_distribution = (
            target_series.astype(str).value_counts().head(20).to_dict()
        )

    analysis_fit = "Profiling and data quality diagnostics"

    preview_rows = _json_safe(df.head(10).to_dict(orient="records"))

    duplicates = int(df.duplicated().sum())
    schema_drift = _schema_drift(df, previous_columns)
    quality_score = _compute_quality_score(df, missing, duplicates, outliers, schema_drift)
    root_cause = _root_cause_and_blast_radius(
        quality_score, missing, duplicates, outliers, label_distribution
    )
    remediation = _auto_remediation_plan(missing, duplicates, outliers)

    grain = "One row per record; columns are fields"
    numeric_correlations = _numeric_correlation_matrix(df, numeric_cols)

    return {
        "data_type": data_type,
        "rows": int(rows),
        "columns": int(cols),
        "column_names": [str(c) for c in df.columns.tolist()],
        "grain": grain,
        "missing_values": missing,
        "duplicates": duplicates,
        "schema_drift": schema_drift,
        "data_quality_score": quality_score,
        "root_cause_blast_radius": root_cause,
        "auto_remediation": remediation,
        "numeric_distributions": distributions,
        "numeric_histograms": histograms,
        "distribution_labels": distribution_labels,
        "numeric_correlations": numeric_correlations,
        "outlier_summary": outliers,
        "categorical_top_values": categorical_summaries,
        "column_profiles": column_profiles,
        "suggestions": suggestions,
        "label_distribution": label_distribution,
        "analysis_fit": analysis_fit,
        "preview": preview_rows,
        "ai_insights": {
            "status": "not_configured",
            "message": "AI insights are not enabled in this local build.",
            "next_steps": [
                "Send summary + profiling stats to an LLM endpoint",
                "Ask for insights, blast radius, and root cause hypotheses",
                "Return prioritized remediation steps and guidance",
            ],
        },
    }


def _analyze_docx(data: bytes) -> Dict[str, Any]:
    doc = Document(io.BytesIO(data))
    paragraphs = [p.text.strip() for p in doc.paragraphs if p.text.strip()]
    word_count = sum(len(p.split()) for p in paragraphs)
    suggestions = [
        "Keyword frequency and themes",
        "Entity extraction",
        "Sentiment analysis",
        "Summarization",
    ]
    return {
        "data_type": "qualitative",
        "rows": len(paragraphs),
        "columns": 1,
        "column_names": ["paragraph"],
        "grain": "One paragraph per record",
        "missing_values": {},
        "duplicates": 0,
        "schema_drift": {"status": "not_available", "drift_rate": 0.0},
        "data_quality_score": {
            "score": 100,
            "weights": {"missing": 0.35, "duplicates": 0.2, "outliers": 0.25, "schema_drift": 0.2},
            "missing_rate": 0.0,
            "duplicate_rate": 0.0,
            "outlier_rate": 0.0,
            "schema_drift_rate": 0.0,
        },
        "root_cause_blast_radius": {
            "risk_severity": "low",
            "top_missing_columns": [],
            "outlier_columns": [],
            "impacted_kpis": [],
            "root_cause_hypotheses": [],
        },
        "auto_remediation": {
            "steps": ["No remediation needed. Data quality is strong."],
            "one_click_actions": ["Remove duplicates", "Impute missing values", "Cap outliers"],
        },
        "numeric_distributions": {},
        "numeric_histograms": {},
        "distribution_labels": {},
        "numeric_correlations": {},
        "outlier_summary": {},
        "categorical_top_values": {},
        "column_profiles": {},
        "suggestions": suggestions,
        "label_distribution": {},
        "analysis_fit": "Text quality profiling",
        "preview": [{"paragraph": p} for p in paragraphs[:10]],
        "ai_insights": {
            "status": "not_configured",
            "message": "AI insights are not enabled in this local build.",
            "next_steps": [
                "Send sample paragraphs + summary to an LLM endpoint",
                "Ask for key themes, risks, and suggested actions",
                "Return an executive summary for stakeholders",
            ],
        },
        "text_summary": {
            "paragraphs": len(paragraphs),
            "word_count": int(word_count),
        },
    }


def _parse_columns(value: str, df: pd.DataFrame) -> List[str]:
    if not value:
        return df.columns.tolist()
    cols = [c.strip() for c in value.split(",") if c.strip()]
    return [c for c in cols if c in df.columns]


def _apply_transform_operations(
    df: pd.DataFrame,
    *,
    drop_duplicates: bool,
    missing_strategy: str,
    fill_value: str,
    target_columns: str,
    normalize_text: str,
    cap_outliers: bool,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    rows_before = int(len(df))
    missing_before = int(df.isna().sum().sum())
    duplicates_before = int(df.duplicated().sum())

    if drop_duplicates:
        df = df.drop_duplicates()

    columns = _parse_columns(target_columns, df)
    if not columns:
        columns = df.columns.tolist()

    if missing_strategy == "drop":
        df = df.dropna(subset=columns, how="any")
    elif missing_strategy in ["mean", "median"]:
        for col in columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                value = df[col].mean() if missing_strategy == "mean" else df[col].median()
                df[col] = df[col].fillna(value)
    elif missing_strategy == "mode":
        for col in columns:
            if df[col].dropna().empty:
                continue
            mode_value = df[col].mode().iloc[0]
            df[col] = df[col].fillna(mode_value)
    elif missing_strategy == "value":
        for col in columns:
            value: Any = fill_value
            if pd.api.types.is_numeric_dtype(df[col]):
                try:
                    value = float(fill_value)
                except ValueError:
                    pass
            df[col] = df[col].fillna(value)

    normalize_text = (normalize_text or "none").strip().lower()
    text_values_changed = 0
    if normalize_text in {"lower", "upper", "title"}:
        for col in columns:
            if pd.api.types.is_numeric_dtype(df[col]) or pd.api.types.is_datetime64_any_dtype(df[col]):
                continue
            mask = df[col].notna()
            if not bool(mask.any()):
                continue
            original = df.loc[mask, col].astype(str)
            normalized = original.str.strip()
            if normalize_text == "lower":
                normalized = normalized.str.lower()
            elif normalize_text == "upper":
                normalized = normalized.str.upper()
            else:
                normalized = normalized.str.title()
            text_values_changed += int((original != normalized).sum())
            df.loc[mask, col] = normalized

    outlier_values_capped = 0
    if cap_outliers:
        for col in columns:
            if not pd.api.types.is_numeric_dtype(df[col]):
                continue
            numeric_series = pd.to_numeric(df[col], errors="coerce")
            clean = numeric_series.dropna()
            if clean.empty:
                continue
            q1 = clean.quantile(0.25)
            q3 = clean.quantile(0.75)
            iqr = q3 - q1
            if iqr <= 0:
                continue
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
            mask = numeric_series.notna() & ((numeric_series < lower) | (numeric_series > upper))
            outlier_values_capped += int(mask.sum())
            if outlier_values_capped > 0:
                df.loc[mask, col] = numeric_series.loc[mask].clip(lower=lower, upper=upper)

    rows_after = int(len(df))
    missing_after = int(df.isna().sum().sum())
    duplicates_after = int(df.duplicated().sum())

    summary = {
        "rows_before": rows_before,
        "rows_after": rows_after,
        "missing_before": missing_before,
        "missing_after": missing_after,
        "duplicates_before": duplicates_before,
        "duplicates_after": duplicates_after,
        "duplicates_removed": max(0, duplicates_before - duplicates_after),
        "target_columns_scope": ", ".join(columns[:12]),
        "text_normalization": normalize_text,
        "text_values_changed": text_values_changed,
        "outlier_capping": "iqr" if cap_outliers else "disabled",
        "outlier_values_capped": outlier_values_capped,
    }
    return df, summary


def _analyze_from_temp_path(
    *,
    temp_path: str,
    ext: str,
    cache_key: str,
    target_column: str,
) -> Dict[str, Any]:
    if ext == ".docx":
        data = _read_file_bytes(temp_path)
        return _execute_with_timeout(
            _analyze_docx,
            data,
            timeout_sec=ANALYZE_TIMEOUT_SEC,
            retries=0,
        )

    data = _read_file_bytes(temp_path)
    df = _execute_with_timeout(
        _read_tabular,
        ext,
        data,
        timeout_sec=ANALYZE_TIMEOUT_SEC,
        retries=1,
    )
    previous_columns = _get_previous_schema(cache_key)
    analysis = _execute_with_timeout(
        _analyze_dataframe,
        df,
        timeout_sec=ANALYZE_TIMEOUT_SEC,
        retries=0,
        target_column=target_column,
        previous_columns=previous_columns,
    )
    _set_previous_schema(cache_key, [str(c) for c in df.columns.tolist()])
    return analysis


def _run_analyze_job(
    job_id: str,
    *,
    temp_path: str,
    file_name: str,
    nature: str,
    ext: str,
    analysis_intent: str,
    target_column: str,
    cache_key: str,
) -> None:
    with analyze_jobs_lock:
        analyze_jobs[job_id] = {
            "status": "running",
            "job_id": job_id,
            "file_name": file_name,
            "started_at": time.time(),
        }

    try:
        analysis = _analyze_from_temp_path(
            temp_path=temp_path,
            ext=ext,
            cache_key=cache_key,
            target_column=target_column,
        )
        result = _json_safe(
            {
                "file_name": file_name,
                "nature": nature,
                "analysis_intent": analysis_intent,
                "target_column": target_column,
                **analysis,
            }
        )
        with analyze_jobs_lock:
            analyze_jobs[job_id] = {
                "status": "done",
                "job_id": job_id,
                "file_name": file_name,
                "completed_at": time.time(),
                "result": result,
            }
    except TimeoutError as exc:
        with analyze_jobs_lock:
            analyze_jobs[job_id] = {
                "status": "error",
                "job_id": job_id,
                "file_name": file_name,
                "completed_at": time.time(),
                "error": {
                    "code": "PROCESSING_TIMEOUT",
                    "message": "Analysis timed out.",
                    "details": str(exc),
                },
            }
    except Exception as exc:  # noqa: BLE001
        with analyze_jobs_lock:
            analyze_jobs[job_id] = {
                "status": "error",
                "job_id": job_id,
                "file_name": file_name,
                "completed_at": time.time(),
                "error": {
                    "code": "PARSE_FAILED",
                    "message": "Failed to parse file",
                    "details": str(exc),
                },
            }
    finally:
        _remove_temp_file(temp_path)


@app.post("/api/analyze")
async def analyze(
    request: Request,
    file: UploadFile = File(...),
    analysis_intent: str = Form(""),
    target_column: str = Form(""),
):
    ext = "." + file.filename.split(".")[-1].lower() if "." in file.filename else ""
    nature = _nature_from_ext(ext)
    if ext not in ALLOWED_EXTENSIONS:
        return _error_response("UNSUPPORTED_TYPE", "Unsupported file type", 400, {"nature": nature})

    temp_path: Optional[str] = None
    try:
        temp_path, size_bytes = await _save_upload_to_tempfile(file, MAX_FILE_SIZE_BYTES, suffix=ext)
    except UploadTooLarge:
        return _error_response(
            "FILE_TOO_LARGE",
            f"File exceeds {MAX_FILE_SIZE_MB}MB limit",
            413,
        )

    _record_upload(size_bytes)

    cache_key = _schema_cache_key(request, file.filename or "unknown")
    try:
        analysis = _analyze_from_temp_path(
            temp_path=temp_path,
            ext=ext,
            cache_key=cache_key,
            target_column=target_column,
        )
    except TimeoutError as exc:
        return _error_response("PROCESSING_TIMEOUT", "Analysis timed out.", 504, str(exc))
    except Exception as exc:  # noqa: BLE001
        return _error_response("PARSE_FAILED", "Failed to parse file", 400, str(exc))
    finally:
        _remove_temp_file(temp_path)

    response = {
        "file_name": file.filename,
        "nature": nature,
        "analysis_intent": analysis_intent,
        "target_column": target_column,
        **analysis,
    }
    return _json_safe(response)


@app.post("/api/analyze/async")
async def analyze_async(
    request: Request,
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    analysis_intent: str = Form(""),
    target_column: str = Form(""),
):
    ext = "." + file.filename.split(".")[-1].lower() if "." in file.filename else ""
    nature = _nature_from_ext(ext)
    if ext not in ALLOWED_EXTENSIONS:
        return _error_response("UNSUPPORTED_TYPE", "Unsupported file type", 400, {"nature": nature})

    temp_path: Optional[str] = None
    try:
        temp_path, size_bytes = await _save_upload_to_tempfile(file, MAX_FILE_SIZE_BYTES, suffix=ext)
    except UploadTooLarge:
        return _error_response(
            "FILE_TOO_LARGE",
            f"File exceeds {MAX_FILE_SIZE_MB}MB limit",
            413,
        )

    _record_upload(size_bytes)

    cache_key = _schema_cache_key(request, file.filename or "unknown")
    job_id = str(uuid.uuid4())
    with analyze_jobs_lock:
        analyze_jobs[job_id] = {
            "status": "queued",
            "job_id": job_id,
            "file_name": file.filename,
            "created_at": time.time(),
        }

    background_tasks.add_task(
        _run_analyze_job,
        job_id,
        temp_path=temp_path,
        file_name=file.filename,
        nature=nature,
        ext=ext,
        analysis_intent=analysis_intent,
        target_column=target_column,
        cache_key=cache_key,
    )

    return {"status": "queued", "job_id": job_id, "file_name": file.filename}


@app.get("/api/analyze/status/{job_id}")
async def analyze_status(job_id: str):
    with analyze_jobs_lock:
        job = analyze_jobs.get(job_id)
    if not job:
        return _error_response("JOB_NOT_FOUND", "Analysis job not found.", 404)
    return job


@app.post("/api/eda-ml")
async def eda_ml(
    file: UploadFile = File(...),
    target_column: str = Form(""),
    time_column: str = Form(""),
    model_list: str = Form(""),
    test_size: str = Form("0.2"),
    cv_folds: str = Form("3"),
):
    return _error_response(
        "MODELING_DISABLED",
        "Modeling features have been removed from this build.",
        410,
    )

    ext = "." + file.filename.split(".")[-1].lower() if "." in file.filename else ""
    nature = _nature_from_ext(ext)
    if ext not in ALLOWED_EXTENSIONS or ext == ".docx":
        return _error_response(
            "UNSUPPORTED_TYPE",
            "EDA/ML supports tabular data only.",
            400,
            {"nature": nature},
        )

    try:
        data = await _read_upload_with_limit(file, MAX_FILE_SIZE_BYTES)
    except UploadTooLarge:
        return _error_response(
            "FILE_TOO_LARGE",
            f"File exceeds {MAX_FILE_SIZE_MB}MB limit",
            413,
        )

    _record_upload(len(data))

    try:
        df = _read_tabular(ext, data)
    except Exception as exc:  # noqa: BLE001
        return _error_response("PARSE_FAILED", "Failed to parse file", 400, str(exc))

    df = _sample_df(df, MAX_ML_ROWS)

    try:
        test_size_val = float(test_size)
    except ValueError:
        test_size_val = 0.2
    test_size_val = min(max(test_size_val, 0.1), 0.5)

    try:
        cv_folds_val = int(cv_folds)
    except ValueError:
        cv_folds_val = 3
    cv_folds_val = min(max(cv_folds_val, 2), 10)

    model_names = [m.strip().lower() for m in model_list.split(",") if m.strip()]

    eda = _light_eda(df, target_column)
    model_results = _train_models(
        df,
        target_column,
        model_names=model_names,
        test_size=test_size_val,
        cv_folds=cv_folds_val,
    )
    pca_results = _compute_pca(df)
    ts_results = _time_series_baselines(df, time_column, target_column)

    return {
        "file_name": file.filename,
        "nature": nature,
        "rows_used": int(len(df)),
        "eda": eda,
        "models": model_results,
        "pca": pca_results,
        "time_series": ts_results,
    }


class ReportPayload(BaseModel):
    analysis: Dict[str, Any]


class AiPayload(BaseModel):
    analysis: Dict[str, Any]


class ChatPayload(BaseModel):
    analysis: Dict[str, Any]
    question: str
    history: List[Dict[str, str]] = []


class ContactPayload(BaseModel):
    name: str
    email: str
    message: str


class SessionLoginPayload(BaseModel):
    username: str
    password: str


@app.post("/api/session/login")
async def session_login(payload: SessionLoginPayload):
    if not AUTH_REQUIRED and not AUTH_PASSWORD:
        return _error_response(
            "AUTH_NOT_ENABLED",
            "Session auth is disabled. Set AUTH_REQUIRED=true and AUTH_PASSWORD.",
            400,
        )
    if not AUTH_PASSWORD:
        return _error_response("AUTH_NOT_CONFIGURED", "AUTH_PASSWORD is not configured.", 503)

    username = payload.username.strip()
    password = payload.password
    if username != AUTH_USER or password != AUTH_PASSWORD:
        return _error_response("UNAUTHORIZED", "Invalid username or password.", 401)

    session = _create_session(username)
    return {
        "status": "ok",
        "token_type": "bearer",
        "token": session["token"],
        "expires_in_seconds": SESSION_TTL_MINUTES * 60,
        "user": username,
    }


@app.post("/api/session/refresh")
async def session_refresh(request: Request):
    token = _extract_bearer_token(request)
    session = _validate_session(token)
    if not session:
        return _error_response("UNAUTHORIZED", "Invalid or expired session token.", 401)

    new_exp = time.time() + (SESSION_TTL_MINUTES * 60)
    with sessions_lock:
        sessions[token]["expires_at"] = new_exp
    return {"status": "ok", "expires_in_seconds": SESSION_TTL_MINUTES * 60}


@app.post("/api/session/logout")
async def session_logout(request: Request):
    token = _extract_bearer_token(request)
    _revoke_session(token)
    return {"status": "logged_out"}


@app.get("/api/session/me")
async def session_me(request: Request):
    token = _extract_bearer_token(request)
    session = _validate_session(token)
    if not session:
        return {"authenticated": False}
    return {
        "authenticated": True,
        "user": session.get("username", ""),
        "expires_at": int(session.get("expires_at", 0)),
    }


def _report_rows(analysis: Dict[str, Any]) -> List[Tuple[str, str]]:
    rows: List[Tuple[str, str]] = []
    rows.append(("File", str(analysis.get("file_name", ""))))
    rows.append(("Nature", str(analysis.get("nature", ""))))
    rows.append(("Data type", str(analysis.get("data_type", ""))))
    rows.append(("Rows", str(analysis.get("rows", ""))))
    rows.append(("Columns", str(analysis.get("columns", ""))))
    rows.append(("Grain", str(analysis.get("grain", ""))))
    rows.append(("Analysis intent", str(analysis.get("analysis_intent", ""))))
    return rows


@app.post("/api/report/csv")
async def report_csv(payload: ReportPayload):
    analysis = payload.analysis
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["section", "key", "value"])

    for key, value in _report_rows(analysis):
        writer.writerow(["overview", _neutralize_csv_cell(key), _neutralize_csv_cell(value)])

    missing = analysis.get("missing_values", {})
    for key, value in missing.items():
        writer.writerow(["missing_values", _neutralize_csv_cell(key), _neutralize_csv_cell(value)])

    profiles = analysis.get("column_profiles", {})
    for col, info in profiles.items():
        writer.writerow(["column_profile", _neutralize_csv_cell(f"{col}_type"), _neutralize_csv_cell(info.get("type", ""))])
        writer.writerow([
            "column_profile",
            _neutralize_csv_cell(f"{col}_missing_rate"),
            _neutralize_csv_cell(info.get("missing_rate", "")),
        ])
        writer.writerow([
            "column_profile",
            _neutralize_csv_cell(f"{col}_unique"),
            _neutralize_csv_cell(info.get("unique", "")),
        ])
        anomalies = "; ".join(info.get("anomalies", []))
        writer.writerow([
            "column_profile",
            _neutralize_csv_cell(f"{col}_anomalies"),
            _neutralize_csv_cell(anomalies),
        ])

    data = output.getvalue().encode("utf-8")
    return Response(
        content=data,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=cigma_data_profiler_report.csv"},
    )


@app.post("/api/report/pdf")
async def report_pdf(payload: ReportPayload):
    from datetime import datetime

    analysis = payload.analysis
    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter
    generated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    def _new_page() -> float:
        pdf.setFillColorRGB(0.10, 0.26, 0.66)
        pdf.rect(0, height - 86, width, 86, stroke=0, fill=1)
        pdf.setFillColorRGB(1, 1, 1)
        pdf.setFont("Helvetica-Bold", 18)
        pdf.drawString(36, height - 42, "CIGMA Data Profiler")
        pdf.setFont("Helvetica", 10)
        pdf.drawRightString(width - 36, height - 42, f"Generated: {generated_at}")
        pdf.setFont("Helvetica", 11)
        pdf.drawString(36, height - 62, "Data Quality Assessment Report")
        pdf.setFillColorRGB(0.07, 0.10, 0.18)
        return height - 110

    def _section_title(y: float, title: str) -> float:
        if y < 120:
            pdf.showPage()
            y = _new_page()
        pdf.setFillColorRGB(0.93, 0.96, 1)
        pdf.roundRect(28, y - 24, width - 56, 26, 7, stroke=0, fill=1)
        pdf.setFillColorRGB(0.12, 0.30, 0.66)
        pdf.setFont("Helvetica-Bold", 12)
        pdf.drawString(38, y - 16, title)
        pdf.setFillColorRGB(0.07, 0.10, 0.18)
        return y - 34

    def _key_values(y: float, items: List[Tuple[str, str]], max_items: int = 12) -> float:
        pdf.setFont("Helvetica", 10)
        for key, value in items[:max_items]:
            if y < 80:
                pdf.showPage()
                y = _new_page()
            key_text = str(key)
            value_text = str(value)[:110]
            pdf.setFillColorRGB(0.29, 0.35, 0.43)
            pdf.drawString(38, y, key_text)
            pdf.setFillColorRGB(0.07, 0.10, 0.18)
            pdf.drawString(220, y, value_text)
            y -= 16
        return y

    y = _new_page()

    y = _section_title(y, "Overview")
    y = _key_values(y, _report_rows(analysis), max_items=20)

    quality = analysis.get("data_quality_score", {})
    y -= 6
    y = _section_title(y, "Quality Summary")
    quality_rows = [
        ("Overall score", quality.get("score", "-")),
        ("Missing rate", quality.get("missing_rate", "-")),
        ("Duplicate rate", quality.get("duplicate_rate", "-")),
        ("Outlier rate", quality.get("outlier_rate", "-")),
        ("Schema drift rate", quality.get("schema_drift_rate", "-")),
    ]
    y = _key_values(y, quality_rows)

    missing = analysis.get("missing_values", {})
    y -= 6
    y = _section_title(y, "Missing Values (Top 20)")
    if missing:
        missing_rows = sorted(missing.items(), key=lambda kv: kv[1], reverse=True)
        y = _key_values(y, [(k, str(v)) for k, v in missing_rows], max_items=20)
    else:
        y = _key_values(y, [("Status", "No missing values detected")], max_items=1)

    root = analysis.get("root_cause_blast_radius", {})
    y -= 6
    y = _section_title(y, "Root Cause and Blast Radius")
    root_rows = [
        ("Risk severity", root.get("risk_severity", "-")),
        ("Impacted KPIs", ", ".join(root.get("impacted_kpis", [])) or "None"),
        ("Top missing columns", ", ".join([f"{c}:{n}" for c, n in root.get("top_missing_columns", [])]) or "None"),
        ("Outlier columns", ", ".join(root.get("outlier_columns", [])) or "None"),
    ]
    y = _key_values(y, root_rows, max_items=10)

    remediation = analysis.get("auto_remediation", {})
    y -= 6
    y = _section_title(y, "Recommended Remediation")
    steps = remediation.get("steps", [])
    if not steps:
        steps = ["No remediation steps available"]
    numbered = [(f"Step {idx}", step) for idx, step in enumerate(steps, start=1)]
    y = _key_values(y, numbered, max_items=15)

    pdf.showPage()
    pdf.save()
    buffer.seek(0)
    return Response(
        content=buffer.read(),
        media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=cigma_data_profiler_report.pdf"},
    )


@app.post("/api/ai-insights")
async def ai_insights(payload: AiPayload):
    if not OPENAI_API_KEY:
        return _error_response(
            "AI_NOT_CONFIGURED",
            "OPENAI_API_KEY is not set on the server.",
            400,
        )
    try:
        return _generate_ai_insights(payload.analysis)
    except requests.RequestException as exc:
        return _error_response("AI_REQUEST_FAILED", "Failed to call OpenAI API.", 502, str(exc))
    except Exception as exc:  # noqa: BLE001
        return _error_response("AI_RESPONSE_ERROR", "OpenAI API returned an error.", 502, str(exc))


@app.post("/api/ai-insights/async")
async def ai_insights_async(payload: AiPayload, background_tasks: BackgroundTasks):
    if not OPENAI_API_KEY:
        return _error_response(
            "AI_NOT_CONFIGURED",
            "OPENAI_API_KEY is not set on the server.",
            400,
        )
    job_id = str(uuid.uuid4())
    with jobs_lock:
        ai_jobs[job_id] = {"status": "queued", "created_at": time.time()}
    background_tasks.add_task(_run_ai_job, job_id, payload.analysis)
    return {"job_id": job_id, "status": "queued"}


@app.post("/api/chat-assistant")
async def chat_assistant(payload: ChatPayload):
    if not OPENAI_API_KEY:
        return _error_response(
            "AI_NOT_CONFIGURED",
            "OPENAI_API_KEY is not set on the server.",
            400,
        )

    question = (payload.question or "").strip()
    if not question:
        return _error_response("INVALID_QUESTION", "Question cannot be empty.", 400)

    summary = _build_ai_summary(payload.analysis)
    history = payload.history[-6:]
    history_text = "\n".join(
        [f"{item.get('role', 'user')}: {item.get('content', '')}" for item in history]
    )

    system_prompt = (
        "You are a concise data-quality copilot. Answer based only on the provided summary. "
        "If a request needs unavailable data, say what is missing."
    )
    user_prompt = textwrap.dedent(
        f"""
        Dataset summary:
        {json.dumps(summary, indent=2)}

        Recent conversation:
        {history_text if history_text else "none"}

        User question:
        {question}
        """
    ).strip()

    payload_body = {
        "model": OPENAI_MODEL,
        "input": [
            {"role": "system", "content": [{"type": "input_text", "text": system_prompt}]},
            {"role": "user", "content": [{"type": "input_text", "text": user_prompt}]},
        ],
        "temperature": 0.2,
    }

    try:
        response = requests.post(
            "https://api.openai.com/v1/responses",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json=payload_body,
            timeout=OPENAI_TIMEOUT,
        )
    except requests.RequestException as exc:
        return _error_response("AI_REQUEST_FAILED", "Failed to call OpenAI API.", 502, str(exc))

    if response.status_code >= 400:
        return _error_response(
            "AI_RESPONSE_ERROR",
            "OpenAI API returned an error.",
            502,
            response.text,
        )

    data = response.json()
    answer = _extract_output_text(data) or "No response generated."
    return {
        "answer": answer,
        "model": OPENAI_MODEL,
        "request_id": response.headers.get("x-request-id", ""),
    }


@app.get("/api/ai-insights/status/{job_id}")
async def ai_insights_status(job_id: str):
    with jobs_lock:
        job = ai_jobs.get(job_id)
    if not job:
        return _error_response("JOB_NOT_FOUND", "AI job not found.", 404)
    return job


@app.post("/api/transform")
async def transform(
    file: UploadFile = File(...),
    drop_duplicates: bool = Form(False),
    missing_strategy: str = Form("none"),
    fill_value: str = Form(""),
    target_columns: str = Form(""),
    normalize_text: str = Form("none"),
    cap_outliers: bool = Form(False),
):
    ext = "." + file.filename.split(".")[-1].lower() if "." in file.filename else ""
    nature = _nature_from_ext(ext)
    if ext not in ALLOWED_EXTENSIONS or ext == ".docx":
        return _error_response(
            "UNSUPPORTED_TRANSFORM",
            "Transformation supports tabular data only.",
            400,
            {"nature": nature},
        )

    temp_path: Optional[str] = None
    try:
        temp_path, size_bytes = await _save_upload_to_tempfile(file, MAX_FILE_SIZE_BYTES, suffix=ext)
    except UploadTooLarge:
        return _error_response(
            "FILE_TOO_LARGE",
            f"File exceeds {MAX_FILE_SIZE_MB}MB limit",
            413,
        )

    _record_upload(size_bytes)

    try:
        data = _read_file_bytes(temp_path)
        df = _execute_with_timeout(
            _read_tabular,
            ext,
            data,
            timeout_sec=TRANSFORM_TIMEOUT_SEC,
            retries=TRANSFORM_RETRY_COUNT,
        )
        df, transform_summary = _execute_with_timeout(
            _apply_transform_operations,
            df,
            timeout_sec=TRANSFORM_TIMEOUT_SEC,
            retries=TRANSFORM_RETRY_COUNT,
            drop_duplicates=drop_duplicates,
            missing_strategy=missing_strategy,
            fill_value=fill_value,
            target_columns=target_columns,
            normalize_text=normalize_text,
            cap_outliers=cap_outliers,
        )
    except TimeoutError as exc:
        return _error_response("PROCESSING_TIMEOUT", "Transform timed out.", 504, str(exc))
    except Exception as exc:  # noqa: BLE001
        return _error_response("PARSE_FAILED", "Failed to parse file", 400, str(exc))
    finally:
        _remove_temp_file(temp_path)

    return {
        "file_name": file.filename,
        "nature": nature,
        "transform_summary": transform_summary,
        "preview": _json_safe(df.head(10).to_dict(orient="records")),
    }


@app.post("/api/transform/download")
async def transform_download(
    file: UploadFile = File(...),
    drop_duplicates: bool = Form(False),
    missing_strategy: str = Form("none"),
    fill_value: str = Form(""),
    target_columns: str = Form(""),
    normalize_text: str = Form("none"),
    cap_outliers: bool = Form(False),
):
    ext = "." + file.filename.split(".")[-1].lower() if "." in file.filename else ""
    nature = _nature_from_ext(ext)
    if ext not in ALLOWED_EXTENSIONS or ext == ".docx":
        return _error_response(
            "UNSUPPORTED_TRANSFORM",
            "Transformation supports tabular data only.",
            400,
            {"nature": nature},
        )

    temp_path: Optional[str] = None
    try:
        temp_path, size_bytes = await _save_upload_to_tempfile(file, MAX_FILE_SIZE_BYTES, suffix=ext)
    except UploadTooLarge:
        return _error_response(
            "FILE_TOO_LARGE",
            f"File exceeds {MAX_FILE_SIZE_MB}MB limit",
            413,
        )

    _record_upload(size_bytes)

    try:
        data = _read_file_bytes(temp_path)
        df = _execute_with_timeout(
            _read_tabular,
            ext,
            data,
            timeout_sec=TRANSFORM_TIMEOUT_SEC,
            retries=TRANSFORM_RETRY_COUNT,
        )
        df, _ = _execute_with_timeout(
            _apply_transform_operations,
            df,
            timeout_sec=TRANSFORM_TIMEOUT_SEC,
            retries=TRANSFORM_RETRY_COUNT,
            drop_duplicates=drop_duplicates,
            missing_strategy=missing_strategy,
            fill_value=fill_value,
            target_columns=target_columns,
            normalize_text=normalize_text,
            cap_outliers=cap_outliers,
        )
    except TimeoutError as exc:
        return _error_response("PROCESSING_TIMEOUT", "Transform timed out.", 504, str(exc))
    except Exception as exc:  # noqa: BLE001
        return _error_response("PARSE_FAILED", "Failed to parse file", 400, str(exc))
    finally:
        _remove_temp_file(temp_path)

    csv_data = df.to_csv(index=False).encode("utf-8")
    return Response(
        content=csv_data,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=transformed.csv"},
    )


@app.post("/api/contact")
async def contact(payload: ContactPayload):
    name = payload.name.strip()
    email = payload.email.strip()
    message = payload.message.strip()

    if not name or not email or not message:
        return _error_response("INVALID_CONTACT", "Name, email, and message are required.", 400)
    if len(name) > 80 or len(email) > 120 or len(message) > 800:
        return _error_response("INVALID_CONTACT", "Contact payload exceeds allowed lengths.", 400)
    if "@" not in email or "." not in email.split("@")[-1]:
        return _error_response("INVALID_CONTACT", "Invalid email format.", 400)

    return {
        "status": "received",
        "message": "Thanks. Your message has been received.",
        "contact": {"name": name, "email": email},
    }


frontend_dir = os.path.join(os.path.dirname(__file__), "..", "frontend")
frontend_dir = os.path.abspath(frontend_dir)

# Serve static files
app.mount("/static", StaticFiles(directory=frontend_dir), name="static")

@app.get("/api/metrics")
async def get_metrics():
    with metrics_lock:
        count = metrics["upload_count"]
        total = metrics["total_bytes"]
    avg = total / count if count else 0
    return {
        "upload_count": count,
        "total_bytes": total,
        "average_bytes": avg,
        "average_mb": round(avg / (1024 * 1024), 4),
        "max_file_size_mb": MAX_FILE_SIZE_MB,
    }

def _serve_frontend_html(file_name: str):
    try:
        with open(os.path.join(frontend_dir, file_name), "r", encoding="utf-8") as f:
            return Response(content=f.read(), media_type="text/html")
    except FileNotFoundError:
        return _error_response("PAGE_NOT_FOUND", f"{file_name} not found", 404)

@app.get("/")
async def serve_index():
    """Serve index.html"""
    return _serve_frontend_html("index.html")


@app.get("/about")
async def serve_about():
    """Serve about.html"""
    return _serve_frontend_html("about.html")


@app.get("/help")
async def serve_help():
    """Serve help.html"""
    return _serve_frontend_html("help.html")


@app.get("/contact")
async def serve_contact():
    """Serve contact.html"""
    return _serve_frontend_html("contact.html")

@app.get("/styles.css")
async def serve_css():
    """Serve styles.css"""
    try:
        with open(os.path.join(frontend_dir, "styles.css"), "r") as f:
            return Response(content=f.read(), media_type="text/css")
    except FileNotFoundError:
        return {"error": "styles.css not found"}

@app.get("/app.js")
async def serve_js():
    """Serve app.js"""
    try:
        with open(os.path.join(frontend_dir, "app.js"), "r") as f:
            return Response(content=f.read(), media_type="application/javascript")
    except FileNotFoundError:
        return {"error": "app.js not found"}


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    if DEBUG:
        return _error_response("INTERNAL_ERROR", "Unexpected server error", 500, str(exc))
    return _error_response("INTERNAL_ERROR", "Unexpected server error", 500)
