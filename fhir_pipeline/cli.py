"""命令行入口。

示例：
  python -m fhir_pipeline.cli ingest -f patient.json
  python -m fhir_pipeline.cli ingest -f obs.json --if-match 2 --key REQ-42
  python -m fhir_pipeline.cli history Patient P1
  python -m fhir_pipeline.cli deidentify
所有拒绝都会以非零退出码结束，并在 stderr 打印可区分原因与判定依据。
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from .errors import PipelineError
from .service import IngestionService

EXIT_OK = 0
EXIT_REJECTED = 1
EXIT_USAGE = 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fhir-pipeline", description="FHIR 本地接入流程 CLI")
    parser.add_argument("-v", "--verbose", action="store_true", help="打印 DEBUG 日志")
    sub = parser.add_subparsers(dest="command", required=True)

    p_ing = sub.add_parser("ingest", help="合入一个 FHIR 风格资源")
    p_ing.add_argument("-f", "--file", required=True, help="JSON 载荷文件，- 表示 stdin")
    p_ing.add_argument("--if-match", type=int, default=None, help="期望版本号（乐观锁）")
    p_ing.add_argument("--key", dest="idempotency_key", default=None, help="幂等键")
    p_ing.set_defaults(handler=_handle_ingest)

    p_get = sub.add_parser("get", help="读取资源当前版本")
    p_get.add_argument("resource_type")
    p_get.add_argument("resource_id")
    p_get.set_defaults(handler=_handle_get)

    p_hist = sub.add_parser("history", help="查看历史版本链")
    p_hist.add_argument("resource_type")
    p_hist.add_argument("resource_id")
    p_hist.set_defaults(handler=_handle_history)

    p_list = sub.add_parser("list", help="列出库内全部资源")
    p_list.set_defaults(handler=_handle_list)

    p_deid = sub.add_parser("deidentify", help="对库内资源执行幂等去标识化")
    p_deid.set_defaults(handler=_handle_deidentify)

    p_merge = sub.add_parser("merge", help="患者合并：把被合并记录并入保留记录")
    p_merge.add_argument("survivor_id", help="保留记录 id")
    p_merge.add_argument("merged_id", help="被合并记录 id")
    p_merge.add_argument("--survivor-version", type=int, required=True,
                         help="发起人看到的保留记录版本（必填，乐观锁）")
    p_merge.add_argument("--merged-version", type=int, required=True,
                         help="发起人看到的被合并记录版本（必填，乐观锁）")
    p_merge.add_argument("--key", dest="idempotency_key", default=None,
                         help="合并幂等键（缺省按 合并对 派生）")
    p_merge.set_defaults(handler=_handle_merge)
    return parser


def _read_payload(path: str) -> str:
    if path == "-":
        return sys.stdin.read()
    return Path(path).read_text(encoding="utf-8")


def _handle_ingest(svc: IngestionService, args: argparse.Namespace) -> int:
    raw = _read_payload(args.file)
    result = svc.ingest(
        raw,
        expected_version=args.if_match,
        idempotency_key=args.idempotency_key,
    )
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    return EXIT_OK


def _handle_get(svc: IngestionService, args: argparse.Namespace) -> int:
    resource = svc.get(args.resource_type, args.resource_id)
    if resource is None:
        print(f"未找到 {args.resource_type}/{args.resource_id}", file=sys.stderr)
        return EXIT_REJECTED
    print(json.dumps(resource.body, ensure_ascii=False, indent=2))
    return EXIT_OK


def _handle_history(svc: IngestionService, args: argparse.Namespace) -> int:
    history = svc.get_history(args.resource_type, args.resource_id)
    print(json.dumps(
        {
            "resourceType": history.resource_type,
            "id": history.resource_id,
            "currentVersion": history.current_version,
            "versions": [
                {"version": e.version, "versionId": e.version_id,
                 "payloadHash": e.payload_hash, "resource": e.body}
                for e in history.entries
            ],
        },
        ensure_ascii=False, indent=2,
    ))
    return EXIT_OK


def _handle_list(svc: IngestionService, _args: argparse.Namespace) -> int:
    rows = [
        {"resourceType": r.resource_type, "id": r.resource_id,
         "version": r.version, "payloadHash": r.payload_hash}
        for r in svc.store.all_resources()
    ]
    print(json.dumps(rows, ensure_ascii=False, indent=2))
    return EXIT_OK


def _handle_deidentify(svc: IngestionService, _args: argparse.Namespace) -> int:
    print(json.dumps(svc.deidentify_all(), ensure_ascii=False, indent=2))
    return EXIT_OK


def _handle_merge(svc: IngestionService, args: argparse.Namespace) -> int:
    result = svc.merge_patients(
        args.survivor_id,
        args.merged_id,
        expected_survivor_version=args.survivor_version,
        expected_merged_version=args.merged_version,
        idempotency_key=args.idempotency_key,
    )
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    service = IngestionService()
    try:
        return args.handler(service, args)
    except PipelineError as exc:
        print(
            json.dumps(
                {"status": "rejected", "reason": exc.reason.value,
                 "detail": exc.message, "context": exc.context},
                ensure_ascii=False, indent=2,
            ),
            file=sys.stderr,
        )
        return EXIT_REJECTED


if __name__ == "__main__":
    raise SystemExit(main())
