# codex-runner

OpenAI Codex CLI を長時間・非同期で安全に走らせる常駐プロセス `codexd` の設計リポジトリです。

> **Status: Stage 1（常駐プロセス基盤）実装中です。** `internal/` 配下に主要コンポーネント（TaskStore・ContractWriter・実行監視の一部等）は実装済みですが、`codexd` 常駐プロセス全体としてはまだ完成していません。

このプロジェクトは有志による非公式のツールです。OpenAI とは関係がなく、OpenAI が提供・保証するものでもありません。Codex は OpenAI の製品名です。

This is an unofficial, community-built project. It is not affiliated with, endorsed by, or supported by OpenAI. Codex is a product of OpenAI.

## English summary

codex-runner is a design-stage project for `codexd`, a local daemon for long-running, asynchronous OpenAI Codex CLI jobs.
It is primarily intended for orchestrators such as Claude Code that delegate implementation, review, and research work to Codex CLI asynchronously.
It can also be used by any local orchestrator that invokes Codex CLI as a subprocess.
The design addresses the loss of a `codex exec` job when its invoking shell terminates.
It records task state and preserves the existing task-output format for callers.
The proposed implementation uses a separate session, file locks, and Codex JSON events, with an optional pseudo-terminal fallback that is disabled by default.
It is designed for a single local user and a Unix domain socket; it is never a network service.
This repository currently contains design documents and an in-progress Go implementation (Stage 1: daemon foundation).
Core components exist under `internal/`, but the `codexd` daemon as a whole is not yet complete.
Feedback on the design is especially welcome.

## 何を解決するのか

呼び出し元のシェルが終了すると、同期実行中の `codex exec` も道連れで終了し得ます。このとき終了コードや成果物が残らず、外部からは「まだ実行中」なのか「すでに終了した」のかを区別できなくなります。

`codexd` は実行の親を呼び出し元から切り離し、開始時点の状態、途中経過、終了状態をタスクごとに記録するための常駐プロセスです。

## 想定する利用シーン: オーケストレーターから Codex CLI を使う場合

`codexd` が主に想定するのは、Claude Code のような AI コーディングエージェント（オーケストレーター）が、実装・レビュー・調査などの作業を Codex CLI（`codex exec`）へ委譲して非同期に実行させる利用形態です。

このパターンでは、オーケストレーター側のプロセスが Codex CLI の呼び出し元シェルになります。典型的な流れは次のとおりです。

1. オーケストレーターが「バックグラウンドで実行して」という形で Codex CLI のコマンドを起動する
2. Codex CLI は数分〜数十分かけて実装やレビューを行う
3. オーケストレーターが呼び出し元シェルまたはそのプロセスグループを終了させ、Codex CLI の完了を待てないことがある（セッションの終了、接続断、ターミナルの終了など）

このとき Codex CLI が呼び出し元と同じ終了制御下にあると、Codex CLI も終了する可能性があり、途中まで進んでいた作業の終了コードや出力を確実に回収できません。

`codexd` は起動管理下で呼び出し元とは独立して常駐し、Codex CLI を自身の子プロセスとして起動します。そのため Codex CLI は呼び出し元シェルの子孫にならず、呼び出し元が終了しても実行を継続できます。さらに Codex CLI を新しいセッションで起動し、管理側のプロセスグループに対する終了信号から分離します。タスクの状態と成果物は後から確認できます。

- Claude Code に限らず、同一の macOS 上で Codex CLI をサブプロセスとして呼び出すオーケストレーター（他の AI エージェント、ローカルまたはセルフホスト型 CI、カスタムスクリプト等）から利用できる設計です
- 呼び出し元との通信は Unix domain socket 経由の薄い入口が担い、既存の呼び出し規約（出力ファイルの形式・終了コードの意味）を維持したまま、裏側の実行方式だけを差し替えます

## なぜ既存の仕組みで足りないのか

`nohup` と `disown` はセッションやプロセスグループを切り離す手段ではありません。macOS には `setsid` コマンドがなく、起動管理の仕組みはプロセスグループごと終了させる制約もあります。加えて、標準入出力を切り離すと Codex が黙って終了する不具合が報告されており（現行版では未再現。後述）、単純なバックグラウンド実行だけではセッションの分離を保証できません。

## 設計の要点

- 擬似端末は既定では割り当てない。切り離し時に Codex が黙って終了する不具合（現行版では未再現）への保険として、設定で有効化できる形で残す
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
  |- 実行と JSON イベント監視（擬似端末は設定で有効化できる保険として残す）
  |- タイムアウト時の回収と互換出力の書き込み
  v
codex exec --json
```

## ロードマップ

段階 -1 の事前確認から始め、応急処置、常駐プロセスとの併存、全実行系の移行、旧経路の削除へ進む構想です。各段階には切り戻し手段を設けます。詳細は [ロードマップ](docs/roadmap.md) を参照してください。

## 設計書

事故分析、実測した制約、要件、比較した案、推奨設計の全文は [codexd 設計書](docs/design/codexd-design.md) にあります。

## 動作環境

設計の調査は macOS、Go、Codex CLI 0.144.5 を前提に確認しています。現在は Stage 1（常駐プロセス基盤）の実装が進行中です。

## ライセンス

MIT License で公開します。詳細は `LICENSE` を参照してください。
