import os
import signal
import threading
from concurrent import futures
from types import FrameType

import grpc

import schema_registry_pb2 as pb2
import schema_registry_pb2_grpc

Schema = pb2.Schema
Struct = pb2.Struct
Field = pb2.Field
Issue = pb2.CompatibilityIssue


def make_issue(code: int, message: str, struct_name: str, field_id: int = 0) -> Issue:
    return Issue(code=code, message=message, struct_name=struct_name, field_id=field_id)


# ---------------------------------------------------------------------------
# Проверки схем: чистые функции, не зависят от gRPC и хранилища
# ---------------------------------------------------------------------------


def validate_schema(schema: Schema) -> list[Issue]:
    """Проверяет саму схему (без сравнения со старой): дубликаты имён и id."""
    issues: list[Issue] = []

    seen_structs: set[str] = set()
    for struct in schema.structs:
        if struct.name in seen_structs:
            issues.append(make_issue(
                pb2.DUPLICATE_STRUCT_NAME,
                f"struct name '{struct.name}' is used more than once",
                struct.name,
            ))
        seen_structs.add(struct.name)
        issues.extend(validate_struct(struct))

    return issues


def validate_struct(struct: Struct) -> list[Issue]:
    issues: list[Issue] = []
    seen_ids: set[int] = set()
    seen_names: set[str] = set()

    for field in struct.fields:
        if field.id in seen_ids:
            issues.append(make_issue(
                pb2.DUPLICATE_FIELD_ID,
                f"field id {field.id} is used more than once",
                struct.name,
                field.id,
            ))
        if field.name in seen_names:
            issues.append(make_issue(
                pb2.DUPLICATE_FIELD_NAME,
                f"field name '{field.name}' is used more than once",
                struct.name,
                field.id,
            ))
        seen_ids.add(field.id)
        seen_names.add(field.name)

    return issues


def check_compatibility(old: Schema, new: Schema) -> list[Issue]:
    """Сравнивает новую схему со старой и собирает ВСЕ нарушения совместимости.

    Ожидается, что обе схемы уже прошли validate_schema (нет дубликатов).
    """
    issues: list[Issue] = []
    new_structs: dict[str, Struct] = {s.name: s for s in new.structs}

    # Новые struct-ы (которых не было в old) не проверяем: их никто ещё не использует.
    for old_struct in old.structs:
        new_struct = new_structs.get(old_struct.name)
        if new_struct is None:
            # В proto нет отдельного кода для удаления struct-а.
            issues.append(make_issue(
                pb2.REMOVED_REQUIRED_FIELD,
                f"struct '{old_struct.name}' was removed",
                old_struct.name,
            ))
            continue
        issues.extend(check_struct_compatibility(old_struct, new_struct))

    return issues


def check_struct_compatibility(old: Struct, new: Struct) -> list[Issue]:
    issues: list[Issue] = []
    name = old.name

    # Поле идентифицируется по id (так устроен wire-формат), а не по позиции.
    old_by_id: dict[int, Field] = {f.id: f for f in old.fields}
    new_by_id: dict[int, Field] = {f.id: f for f in new.fields}

    for field_id, old_field in old_by_id.items():
        new_field = new_by_id.get(field_id)
        if new_field is None:
            if old_field.required:
                issues.append(make_issue(
                    pb2.REMOVED_REQUIRED_FIELD,
                    f"required field '{old_field.name}' (id={field_id}) was removed",
                    name,
                    field_id,
                ))
            continue

        if new_field.type != old_field.type:
            issues.append(make_issue(
                pb2.FIELD_TYPE_CHANGED,
                f"field id={field_id} changed type from "
                f"{pb2.FieldType.Name(old_field.type)} to {pb2.FieldType.Name(new_field.type)}",
                name,
                field_id,
            ))
        if not old_field.required and new_field.required:
            issues.append(make_issue(
                pb2.OPTIONAL_TO_REQUIRED,
                f"field id={field_id} changed from optional to required",
                name,
                field_id,
            ))

    for field_id, new_field in new_by_id.items():
        if field_id not in old_by_id and new_field.required:
            issues.append(make_issue(
                pb2.ADDED_REQUIRED_FIELD,
                f"new field '{new_field.name}' (id={field_id}) must not be required",
                name,
                field_id,
            ))

    # Отдельно ищем поля, у которых имя осталось, а id поменялся.
    old_by_name: dict[str, Field] = {f.name: f for f in old.fields}
    for new_field in new.fields:
        old_field = old_by_name.get(new_field.name)
        if old_field is not None and old_field.id != new_field.id:
            issues.append(make_issue(
                pb2.FIELD_ID_CHANGED,
                f"field '{new_field.name}' changed id from {old_field.id} to {new_field.id}",
                name,
                new_field.id,
            ))

    return issues


def find_issues(old: Schema | None, new: Schema) -> list[Issue]:
    """Все проблемы кандидата: сначала валидность, затем совместимость со старой схемой."""
    issues = validate_schema(new)
    # Сравнивать невалидную схему бессмысленно: при дубликатах id непонятно, какое поле с каким сравнивать.
    if issues or old is None:
        return issues
    return check_compatibility(old, new)


# ---------------------------------------------------------------------------
# gRPC-сервис
# ---------------------------------------------------------------------------


class SchemaRegistryService(schema_registry_pb2_grpc.SchemaRegistryServicer):
    def __init__(self) -> None:
        # service_name -> список версий; версия N лежит по индексу N-1.
        self._schemas: dict[str, list[Schema]] = {}
        # gRPC-сервер обрабатывает запросы в пуле потоков, поэтому доступ к словарю под мьютексом.
        self._lock = threading.Lock()

    def RegisterSchema(
        self, request: pb2.RegisterSchemaRequest, context: grpc.ServicerContext
    ) -> pb2.RegisterSchemaResponse:
        service_name = self._require_service_name(request.service_name, context)

        # Проверка и добавление под одной блокировкой: иначе два параллельных запроса
        # могут оба сравниться с одной и той же "последней" версией.
        with self._lock:
            versions = self._schemas.setdefault(service_name, [])
            latest = versions[-1] if versions else None

            issues = find_issues(latest, request.schema)
            if issues:
                if not versions:
                    del self._schemas[service_name]  # не создаём пустой сервис
                return pb2.RegisterSchemaResponse(accepted=False, version=len(versions), issues=issues)

            stored = Schema()
            stored.CopyFrom(request.schema)
            versions.append(stored)
            return pb2.RegisterSchemaResponse(accepted=True, version=len(versions))

    def CheckCompatibility(
        self, request: pb2.CheckCompatibilityRequest, context: grpc.ServicerContext
    ) -> pb2.CheckCompatibilityResponse:
        service_name = self._require_service_name(request.service_name, context)

        with self._lock:
            versions = self._get_versions(service_name, context)
            if not 1 <= request.base_version <= len(versions):
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    f"base_version must be in [1, {len(versions)}], got {request.base_version}",
                )
            base = versions[request.base_version - 1]

        issues = find_issues(base, request.candidate_schema)
        return pb2.CheckCompatibilityResponse(compatible=not issues, issues=issues)

    def GetSchema(
        self, request: pb2.GetSchemaRequest, context: grpc.ServicerContext
    ) -> pb2.GetSchemaResponse:
        service_name = self._require_service_name(request.service_name, context)

        with self._lock:
            versions = self._get_versions(service_name, context)
            # version=0 означает "последняя версия".
            version = request.version if request.version != 0 else len(versions)
            if not 1 <= version <= len(versions):
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    f"version must be in [0, {len(versions)}], got {request.version}",
                )
            return pb2.GetSchemaResponse(version=version, schema=versions[version - 1])

    def GetLatestVersion(
        self, request: pb2.GetLatestVersionRequest, context: grpc.ServicerContext
    ) -> pb2.GetLatestVersionResponse:
        service_name = self._require_service_name(request.service_name, context)

        with self._lock:
            versions = self._get_versions(service_name, context)
            return pb2.GetLatestVersionResponse(version=len(versions))

    @staticmethod
    def _require_service_name(service_name: str, context: grpc.ServicerContext) -> str:
        if not service_name.strip():
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "service_name must not be empty")
        return service_name

    def _get_versions(self, service_name: str, context: grpc.ServicerContext) -> list[Schema]:
        """Вызывать под self._lock. context.abort бросает исключение и прерывает RPC."""
        versions = self._schemas.get(service_name)
        if not versions:
            context.abort(grpc.StatusCode.NOT_FOUND, f"service '{service_name}' not found")
        return versions


def serve() -> None:
    port = int(os.getenv("PORT", "50051"))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    schema_registry_pb2_grpc.add_SchemaRegistryServicer_to_server(
        SchemaRegistryService(),
        server,
    )
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"Schema Registry listening on [::]:{port}", flush=True)

    # docker stop шлёт SIGTERM; процесс с PID 1 по умолчанию его игнорирует,
    # поэтому явно останавливаем сервер, давая текущим запросам 5 секунд на завершение.
    def handle_sigterm(signum: int, frame: FrameType | None) -> None:
        server.stop(grace=5)

    signal.signal(signal.SIGTERM, handle_sigterm)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
