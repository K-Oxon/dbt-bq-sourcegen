# dbt-bq-sourcegen

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

BigQueryからdbtのsource YAMLを作成・更新します。

## 概要

`dbt-bq-sourcegen`は、BigQueryスキーマからdbt source YAMLファイルを自動生成し、更新時には既存の設定を保持します。  
ワイルドカードパターンによるテーブルフィルタリングをサポートし、YAMLのフォーマットやコメントを維持します。  
このツールにより、dbtのsource定義とBigQueryデータセットの同期作業を効率化できます。

## インストール

```bash
pip install git+https://github.com/K-Oxon/dbt-bq-sourcegen.git
```

## 使い方

### Apply（自動的に作成または更新）

```bash
dbt-bq-sourcegen apply \
  --project-id your-project \
  --schema your_dataset \
  --output models/staging/your_dataset/src_your_dataset.yml
```

### テーブルフィルタリングを使用する場合

```bash
# パターンに一致するテーブルのみを含める
dbt-bq-sourcegen apply \
  --project-id your-project \
  --schema your_dataset \
  --table-pattern "stg_*" \
  --output models/staging/your_dataset/src_your_dataset.yml

# 特定のテーブルを除外
dbt-bq-sourcegen apply \
  --project-id your-project \
  --schema your_dataset \
  --exclude "temp" \
  --output models/staging/your_dataset/src_your_dataset.yml
```

## オプション

- `--project-id`: Google CloudプロジェクトID（必須）
- `--schema`, `--dataset`: BigQueryスキーマ/データセット名（必須）
- `--output`: 出力YAMLファイルパス（必須）
- `--table-pattern`: テーブル名パターン（例：'stg_*'）
- `--exclude`: 指定文字列を含むテーブルを除外

## 機能

- BigQueryスキーマからdbt source YAMLを自動生成
- 既存のsource YAMLファイルを更新する際、カスタム設定を保持
- ワイルドカードパターンによるテーブルフィルタリング対応
- YAMLのフォーマットとコメントを保持
- クリーンな設計によるPure Python実装

## 開発

```bash
# 開発用依存関係をインストール
uv sync

# テストを実行
uv run pytest

# コードをフォーマット
uv run ruff format src/
```