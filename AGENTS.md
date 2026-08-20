# AGENTS.md — AI 実装者向けの入口

このファイルは日本語を主体とし、要点を英語でも併記する。理由: 本リポジトリの `README.md` /
`CONTRIBUTING.md` が同じ方針（日本語主体・英語要約）を既に採用しており、入口文書の言語方針を
リポジトリ内で統一するため。

This file is Japanese-first with an English summary of the essentials, matching this repository's
existing `README.md` / `CONTRIBUTING.md` convention.

## English summary

- **Status: Stage 1 implementation in progress.** Core components (e.g. task store, contract
  writer, execution monitoring) exist under `internal/`, but the `codexd` daemon as a whole is not
  yet complete. Do not assume every package, type, or function mentioned below is already
  implemented — check the actual source under `internal/` for current status.
- The canonical source for values (enums, thresholds, error codes, message text) and for the API
  contract (socket protocol verbs, CLI subcommands, on-disk output format) lives outside this
  repository, in a local specifications directory that is **not part of the public repository**.
  The public, redacted design context that *is* part of this repository is
  `docs/design/codexd-design.md`.
- Do not hardcode enum values or thresholds you find written out in design docs; treat any such
  value as a copy of a canonical definition, not the definition itself.
- Do not change the on-disk task-output format under `/tmp/codex-tasks/<task-id>/` without an
  explicit instruction — nine external check scripts currently read it directly (see
  `02-domain/output-contract.md` §5 in the specifications directory for the current count), and it
  is treated as a frozen contract.
- Do not write absolute, machine-specific filesystem paths into source code or tests.
- Verification commands are listed below (`go build`, `go vet`, `go test -race`, `golangci-lint`,
  `shellcheck`). Run them before proposing a change is complete.

## このリポジトリが何か

`codexd`（OpenAI Codex CLI を長時間・非同期で安全に走らせる常駐プロセス）の**設計リポジトリ**である。
`README.md` が明記するとおり、現時点は**Stage 1（常駐プロセス基盤）の実装が進行中**である。
`go.mod` と `internal/` 配下に主要コンポーネント（TaskStore・ContractWriter・実行監視の一部等）は
実装済みだが、`codexd` 常駐プロセス全体としてはまだ完成していない。以下で触れる型名・パッケージ名・
関数シグネチャは正典が定める名前を基準としており、実装済みかどうかは `internal/` 配下の実ソースで
都度確認すること。

実装を始める前に、まず `README.md`（何を解決するか）と `docs/roadmap.md`（段階移行の計画）を読むこと。

## 正典の所在（値の実体はここでは書かない）

本プロジェクトの詳細仕様（機能設計・ドメインモデル・値の定義・API 契約・受入シナリオ）は、
**このリポジトリの外にある仕様ディレクトリ**（`.docs/specifications/`）が正本である。
リポジトリを操作している環境にそのディレクトリへのアクセスがあるかどうかを、作業を始める前に
必ず確認すること。アクセスできる場合の参照先は次のとおり。

| 何を確認したいか | 参照先（仕様ディレクトリ配下の相対パス） |
|---|---|
| 実装タスクの分解・実装順序・依存関係 | `07-release/task-breakdown.md` |
| ユースケース・モジュール・永続ファイル・受入シナリオの対応表 | `00-overview/traceability.md` |
| 値の正典（状態・動詞・終了コード分類・エラーコード・閾値・文言） | `10-shared/` 配下（`published-language/`・`message-catalog.md`・`validation-rules.md`） |
| 状態機械（`Task` 集約の状態遷移・不変条件） | `02-domain/models/task.md` |
| 永続ファイルの全フィールド定義 | `02-domain/state-files.md` |
| 出力契約（検査スクリプトが読む後方互換フォーマット。件数の内訳は本表右のファイルの §5 参照。**変更禁止の凍結仕様**） | `02-domain/output-contract.md` |
| API 契約の正本（ソケットの 5 動詞・CLI サブコマンド・出力契約の 3 つ） | `09-functional-design/FD-*.md`（`03-api/` はここからの転写に過ぎない） |
| 受入条件 | `09-functional-design/behaviors/FD-*.behavior.md` の Gherkin シナリオ（SCN ID） |
| ディレクトリ構成・実装マッピング | `01-architecture/directory-map.md` |
| 不可逆な技術決定とその理由 | `01-architecture/adr/*.md` |
| 仕様の変更履歴 | `spec-changelog.md` |

**ただし `.docs/` はこのリポジトリの外にあり、公開リポジトリ（このリポジトリ）には含まれない。**
公開されているのはこのリポジトリの `docs/design/codexd-design.md`（設計書の公開版）であり、
一般の閲覧者・貢献者はこちらを一次情報として読む。上表の仕様ディレクトリへアクセスできない
環境で作業する場合は、`docs/design/codexd-design.md` と `docs/roadmap.md` を正本として扱うこと。

## 守ること

- **値を直書きしない。** enum の値・閾値（タイムアウト秒数・同時実行数の上限等）・エラーコード・
  利用者向け文言を、コード中に生の値として書かない。仕様ディレクトリへアクセスできる場合はそこを
  参照する実装にし、アクセスできない場合はどの値をどこから転記したかをコメントで明示する。
- **技術選定・アーキテクチャを変更しない。** Go の採用、常駐プロセス方式、擬似端末を既定では
  割り当てず設定 `pty_enabled`（既定 `false`）で有効化できる保険として残す方針、ファイルロックに
  よる生死判定、出力契約の凍結は、いずれも `01-architecture/adr/` が定める不可逆な決定である。
  擬似端末を常時有効にする・保険自体を削除するのも、いずれも決定の変更に当たる。変更が必要だと
  思えた場合は、まず Issue で論点を共有すること（`CONTRIBUTING.md` の手順に従う）。
- **出力契約を変えない。** `/tmp/codex-tasks/<task-id>/` 配下のファイル（`task.json` / `prompt.md` /
  `exit-code` / `last-message.md` / `stdout.log` / `stderr.log` 等）の形式・書き込みタイミングは、
  外部検査スクリプト（異なり 15 種類のうち現存 9 本。内訳は `02-domain/output-contract.md` §5 参照）が
  直接読む後方互換契約であり、明示的な指示なしに変更しない。
- **絶対パスを書かない。** ソースコード・テスト・設定に、特定のマシン環境に固有の絶対パス
  （`/Users/...` や `/Volumes/...` など）を書き込まない。
- **仕様にない機能を追加しない。** スコープを勝手に広げない。

## 検証コマンド

対象 OS は macOS のみ（Linux・Windows は対象外）。

```bash
go build ./...
go vet ./...
golangci-lint run
go test -race ./...
```

仕様ディレクトリへアクセスできる環境では、仕様整合の機械検査も実行する。

```bash
# <仕様ディレクトリ>/spec-lint.sh 相当（配置はローカル環境に依存するため固定パスを書かない）
```

## 禁止コマンド

`git commit` / `git push` を含む Git 操作、パッケージの追加・更新、Docker 操作、本番接続に相当する
操作は、明示的な指示がない限り行わない（実装フェーズに入った後の委譲契約に従う）。CI では実際の
Codex CLI（`codex exec`）を呼び出さない（`ProcessRunner` 抽象をフェイクに差し替えてテストする）。
