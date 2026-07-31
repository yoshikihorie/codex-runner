# codex-runner

OpenAI Codex CLI を長時間・非同期で安全に走らせる常駐プロセス `codexd` の設計リポジトリです。

> **Status: 設計フェーズ（実装未着手）です。** 現在、動作する `codexd` や Go の実装はありません。

このプロジェクトは有志による非公式のツールです。OpenAI とは関係がなく、OpenAI が提供・保証するものでもありません。Codex は OpenAI の製品名です。

This is an unofficial, community-built project. It is not affiliated with, endorsed by, or supported by OpenAI. Codex is a product of OpenAI.

## English summary

codex-runner is a design-stage project for `codexd`, a local daemon for long-running, asynchronous OpenAI Codex CLI jobs.
It is intended for developers who run coding agents such as Codex CLI or Claude Code on their own machines.
The design addresses the loss of a `codex exec` job when its invoking shell terminates.
It records task state and preserves the existing task-output format for callers.
The proposed implementation uses a pseudo-terminal, a separate session, file locks, and Codex JSON events.
It is designed for a single local user and a Unix domain socket; it is never a network service.
This repository currently contains design documents only.
Implementation has not started.
Feedback on the design is especially welcome.

## 何を解決するのか

呼び出し元のシェルが終了すると、同期実行中の `codex exec` も道連れで終了し得ます。このとき終了コードや成果物が残らず、外部からは「まだ実行中」なのか「すでに終了した」のかを区別できなくなります。

`codexd` は実行の親を呼び出し元から切り離し、開始時点の状態、途中経過、終了状態をタスクごとに記録するための常駐プロセスです。

## なぜ既存の仕組みで足りないのか

`nohup` と `disown` はセッションやプロセスグループを切り離す手段ではありません。macOS には `setsid` コマンドがなく、起動管理の仕組みはプロセスグループごと終了させる制約もあります。加えて、標準入出力を切り離すと Codex が黙って終了する既知の不具合があり、単純なバックグラウンド実行では安全に解決できません。

## 設計の要点

- 擬似端末を割り当て、切り離し時に Codex が黙って終了する問題を回避する
- 子プロセスを新しいセッションで起動し、管理側の終了信号から分離する
- タスクごとのファイルロックで、生存と死亡を推定ではなく事実として判定する
- `codex exec --json` のイベント列で進捗停滞を判定する
- 既存のタスク出力形式を維持し、利用側の検査を壊さない

## 想定する構成

```text
呼び出し元
  |
  v
薄い入口（互換性と入力検査）
  | Unix domain socket / JSON
  v
codexd
  |- 待ち行列・状態管理・ファイルロック
  |- 擬似端末付きの実行と JSON イベント監視
  |- タイムアウト時の回収と互換出力の書き込み
  v
codex exec --json
```

## ロードマップ

段階 -1 の事前確認から始め、応急処置、常駐プロセスとの併存、全実行系の移行、旧経路の削除へ進む構想です。各段階には切り戻し手段を設けます。詳細は [ロードマップ](docs/roadmap.md) を参照してください。

## 設計書

事故分析、実測した制約、要件、比較した案、推奨設計の全文は [codexd 設計書](docs/design/codexd-design.md) にあります。

## 動作環境

設計の調査は macOS、Go、Codex CLI 0.144.5 を前提に確認しています。実装は未着手です。

## ライセンス

MIT License で公開します。詳細は `LICENSE` を参照してください。
