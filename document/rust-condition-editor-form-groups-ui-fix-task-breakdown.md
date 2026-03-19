# Rust condition editor form_groups UI + legacy 混在修正 タスク分解

- 日付: 2026-03-19
- 前提: [rust-condition-editor-form-groups-ui-fix-design.md](/mnt/f/program_2026/csv_viewer/document/rust-condition-editor-form-groups-ui-fix-design.md) の反映版を実装へ落とす
- 目的:
  - GUI 保存時の `legacy_and_form_groups_mixed` を止める
  - Rust condition editor を `form_groups` 編集 UI へ移行する

## 既存挙動

- `src/condition_editor.rs` は `form_groups` model を保持できる
- しかし `src/app.rs` の editor UI はまだ legacy `forms` 前提
- その結果、legacy condition を保存すると
  - `forms`
  - `form_groups: []`
  が共存し、backend で `legacy_and_form_groups_mixed` error になる

## Phase 0: 契約固定

### Task 0-1: 保存時 schema 契約を固定

- legacy-only condition:
  - `form_groups` を書かない
- form_groups condition:
  - `forms`
  - `form_match_logic`
  - `search_scope`
  - `max_token_distance`
  を書かない

### Task 0-2: migration 通知契約を固定

- 初回 migration 保存時に
  - badge
  - status message
  - 確認文言
  のどれを出すかを固定する

### Task 0-3: group scope UI 契約を固定

- group `search_scope` の選択肢を
  - `(全体設定に従う)`
  - `paragraph`
  - `sentence`
  に固定する
- 保存時は `(全体設定に従う)` を `None` として扱う

### Task 0-4: invalid state 契約を固定

- Group 1 `not` は UI で選べない
- token clause 空 condition は保存禁止
- `anchor_form` 不整合は
  - UI warning
  - 保存時補正または保存ブロック
  に固定する

## Phase 1: 保存事故防止

### Task 1-1: `condition_editor.rs` の serialize 方針を修正

- 空 `form_groups` を保存しない
- `skip_serializing_if` か明示 sanitize のどちらかで固定する

### Task 1-2: `normalize_legacy_fields_for_save(...)` を追加

- legacy-only condition:
  - `form_groups` を消す
- form_groups condition:
  - legacy fields を消す

### Task 1-3: `extra_fields` から legacy keys を除去

- remove 対象:
  - `forms`
  - `form_match_logic`
  - `search_scope`
  - `max_token_distance`
  - `form_groups`

### Task 1-4: token clause 空 validation を追加

- `forms` と `form_groups` が両方空なら保存エラー
- `new_condition` のような空 condition を止める

### Task 1-5: migration 通知 state を追加

- condition ごとの migrated state
- save 後の status message
- dirty state との整合

## Phase 2: legacy projection

### Task 2-1: legacy condition を editor 用 group へ投影

- `forms` -> group1.forms
- `form_match_logic=all` -> `and`
- `form_match_logic=any` -> `or`
- `search_scope` -> group1.search_scope
- `max_token_distance` -> group1.max_token_distance

### Task 2-2: projection state と保存 state を分離

- UI 上では group 化して見せる
- 保存時だけ新 schema / legacy schema を確定する

### Task 2-3: condition list preview を `form_groups` 対応へ変更

- `condition.forms.len()` ではなく
  - group 数
  - total forms 数
  を使う

## Phase 3: Rust UI 基本置換

### Task 3-1: legacy 単体 token editor を撤去

- 削除対象:
  - 単体 `form_match_logic`
  - 単体 `search_scope`
  - 単体 `max_token_distance`
  - 単体 `forms`

### Task 3-2: group list / group card UI を追加

- 1 group = 1 card
- 追加 / 削除 / 複製
- 順序変更

### Task 3-3: single-open accordion か selected-detail を採用

- `CollapsingHeader` 全開放は避ける
- 一度に 1 group だけ詳細表示できる方式へ寄せる

### Task 3-4: group logic editor を追加

- Group 1
  - `and`
  - `or`
- Group 2+
  - `and`
  - `and or`
  - `and not`
  - `or and`
  - `or`
  - `or not`

### Task 3-5: group scope editor を追加

- `(全体設定に従う)`
- `paragraph`
- `sentence`

### Task 3-6: distance / anchor / exclusion editor を追加

- `max_token_distance`
- `anchor_form`
- `exclude_forms_any`

## Phase 4: invalid state UX

### Task 4-1: `anchor_form` invalid warning を追加

- 即時削除しない
- card 内で赤警告表示

### Task 4-2: `not` group の distance UI 制御

- `not` group では distance を disabled
- explanation 文か help text を出す

### Task 4-3: migration badge / save 通知 UI を追加

- legacy 由来 group
- 新 schema へ移行予定
- save 後に形式変更を明示

## Phase 5: 保存結果確認

### Task 5-1: saved JSON が mixed schema にならないことを確認

- legacy-only condition 保存
- form_groups condition 保存
- mixed migration 保存

### Task 5-2: `table` のような legacy 起源 condition が再び動くことを確認

- GUI 保存後に backend で `legacy_and_form_groups_mixed` が消えることを確認

### Task 5-3: unknown field 保持確認

- `extra_fields` 由来の未知キーが失われないこと

## Phase 6: テスト

### Task 6-1: `condition_editor.rs` 単体テスト

- empty `form_groups` non-serialization
- legacy field cleanup
- `extra_fields` cleanup
- token clause empty reject

### Task 6-2: JSON round-trip test

- legacy input -> projection -> save
- form_groups input -> save
- unknown field preserving save

### Task 6-3: Rust 手動確認

- legacy condition を GUI で開いたとき group として見える
- 保存しても `form_groups: []` が書かれない
- form_groups を編集して保存できる
- migration 通知が見える
- `table` が再度ヒットする

## 承認時に見てほしい点

1. 保存事故防止を Phase 1 として最優先に切ってよいか
2. group UI は single-open accordion でよいか
3. migration 通知を save 前 confirmation ではなく badge + status message にしてよいか
4. group scope の既定を `(全体設定に従う)` にしてよいか

---

## セカンドオピニオン / 批判的レビュー (2026-03-19)

タスクブレイクダウンと実際のコード（`condition_editor.rs`, `app.rs`）を比較検討した結果、以下の懸念点を指摘する。

### 1. legacy fields と form_groups の混在防止ロジック欠如 (バグ確定)

**指摘**: `condition_editor.rs:135-174` の `sanitize_document_for_save` は、`form_groups` と legacy fields (`forms`, `form_match_logic`, `search_scope`, `max_token_distance`) の混在を検出・防止するロジックを持っていない。これが mixed schema エラーの根本原因。

**現状コード**:
```rust
// condition_editor.rs:154
sanitize_string_list(&mut condition.forms);
// ...
sanitize_form_groups(&mut condition.form_groups);
```
両方が独立に sanitize されるだけで、混在チェックがない。

**リスク**: Task 1-2 で追加予定の `normalize_legacy_fields_for_save(...)` が必須だが、実装漏れがあれば mixed schema エラーが再発する。

**対策**: 以下のいずれかを強制:
- legacy-only の場合: `form_groups` を `Vec::new()` にしてから `skip_serializing_if` でキー自体を出力しない
- form_groups の場合: legacy fields を `None` / `Vec::new()` にしてから同様にスキップ

### 2. `extra_fields` からの legacy keys 復活 (バグの可能性)

**指摘**: `ConditionEditorItem` は `#[serde(flatten)]` で `extra_fields` を保持している。Task 1-3 で `extra_fields` から legacy keys を除去するとあるが、現在の `sanitize_document_for_save` にこの処理がない。

**現状コード**:
```rust
// condition_editor.rs:53-54
#[serde(default, flatten)]
pub(crate) extra_fields: HashMap<String, Value>,
```

**リスク**: JSON 入力に未知フィールドとして `forms` が含まれていた場合、`extra_fields` 経由でシリアライズ時に再出力され、mixed schema エラーが再発する。

**対策**: `normalize_legacy_fields_for_save` 内で明示的に:
```rust
condition.extra_fields.remove("forms");
condition.extra_fields.remove("form_match_logic");
condition.extra_fields.remove("search_scope");
condition.extra_fields.remove("max_token_distance");
condition.extra_fields.remove("form_groups");
```

### 3. 空の `form_groups` と空の `forms` が両方空の条件 (バグの可能性)

**指摘**: Task 1-4 で「token clause 空 validation を追加」とあるが、現在の `sanitize_document_for_save` にこのチェックがない。

**現状**: `build_default_condition_item()` が `forms: vec![]` を生成するため、新規 condition は token 条件なしで保存可能。

**リスク**: バックエンドが「トークン条件なし」をどう扱うか不明。全パラグラフにヒットする、あるいはエラーになる不整合が起きる可能性。

**対策**: 保存前バリデーションで:
```rust
if condition.forms.is_empty() && condition.form_groups.is_empty() {
    return Err("token 条件が空です".to_string());
}
```

### 4. condition list preview の `forms.len()` (UI崩れ)

**指摘**: `app.rs:1735` で `condition.forms.len()` を使っている。

**現状コード**:
```rust
// app.rs:1731-1735
let label = format!(
    "{}. {} [{}] forms:{} filters:{} refs:{}",
    // ...
    condition.forms.len(),
```

**リスク**: form_groups 対応後、`forms` が空で `form_groups` に forms がある場合、プレビューが「forms:0」となり誤解を招く。

**対策**: Task 2-3 で言及されている通り、total forms 数を計算する関数を追加:
```rust
fn total_forms_count(condition: &ConditionEditorItem) -> usize {
    if !condition.form_groups.is_empty() {
        condition.form_groups.iter().map(|g| g.forms.len()).sum()
    } else {
        condition.forms.len()
    }
}
```

### 5. `build_default_condition_item()` の legacy 形式 (意図しない動作)

**指摘**: `condition_editor.rs:176-183` で新規 condition を legacy 形式で生成している。

**現状コード**:
```rust
ConditionEditorItem {
    condition_id: "new_condition".to_string(),
    overall_search_scope: Some("paragraph".to_string()),
    form_match_logic: Some("all".to_string()),
    search_scope: Some("paragraph".to_string()),
    ..Default::default()
}
```

**リスク**: 新規追加直後に保存すると、legacy-only 形式で保存される。ユーザーが意図せず legacy 形式を使い続けることになる。

**対策**: 新規 condition も form_groups 形式で生成する、または明示的に「新規は legacy 形式」というポリシーを文書化する。

### 6. Group 1 `not` 制約の既存データ対応 (意図しない動作)

**指摘**: Task 0-4 で「Group 1 `not` は UI で選べない」とあるが、既存 JSON に `match_logic=not` の Group 1 があった場合の処理が不明。

**リスク**: 既存データを読み込んだ際、UI 上でどう表示するか。選択不可にするだけで値は保持するのか、それとも強制的に `and` に変更するのか。

**対策**: 読込時に検出して警告表示、または自動で `and` に変更して migration 扱いにする。

### 7. `anchor_form` 不整合の遅延修正タイミング (計算ミス / バグ)

**指摘**: Task 4-1 で「即時削除しない」「保存時にのみ None へ補正」とあるが、保存時の補正ロジックがどこにあるか現状コードにない。

**リスク**: 
- `forms` リストを編集中に `anchor_form` が無効になっても UI 警告のみで、保存時にどう処理されるか不明
- `match_logic = "and"` で `anchor_form` がない場合のフォールバック（例: 0番目を使う）がバックエンド側と一致しているか確認が必要

**対策**: `sanitize_form_groups` 内で `anchor_form` の有効性チェックを追加:
```rust
if let Some(anchor) = &group.anchor_form {
    if !group.forms.contains(anchor) {
        group.anchor_form = None; // または保存エラー
    }
}
```

### 8. `max_token_distance` の i64 型と負の値 (意図しない動作)

**指摘**: `FormGroupEditorItem.max_token_distance` が `Option<i64>` だが、負の値が入力可能。

**現状コード**:
```rust
// condition_editor.rs:69-70
#[serde(default, deserialize_with = "deserialize_optional_i64_from_any")]
pub(crate) max_token_distance: Option<i64>,
```

**リスク**: -1 や負の値が入力された場合、バックエンドがどう扱うか不明。「無効」の意味で -1 を使う慣習がある場合、意図しない挙動になる可能性。

**対策**: UI 側で `>= 0` のバリデーション、または `u32` への型変更を検討。

### 9. `search_scope` のロック条件 (意図しない動作)

**指摘**: `app.rs:1808-1843` で `annotation_filters` がある場合に `search_scope` を `paragraph` 固定にしている。

**現状コード**:
```rust
// app.rs:1808-1816
let search_scope_locked = !condition.annotation_filters.is_empty();
if search_scope_locked {
    if condition.search_scope.as_deref() != Some("paragraph") {
        condition.search_scope = Some("paragraph".to_string());
        changed = true;
    }
```

**リスク**: form_groups 導入後、各 group の `search_scope` も同様にロックすべきか。現在の仕様では condition レベルの `search_scope` のみロックしているが、group レベルの `search_scope` も考慮が必要。

**対策**: form_groups の各 group でも `annotation_filters` 有無をチェックし、group `search_scope` をロックするロジックを追加。

### 10. projection state と保存 state の分離複雑性 (バグの可能性)

**指摘**: Task 2-2 で「projection state と保存 state を分離」とあるが、実装が複雑になる可能性。

**リスク**: 
- legacy JSON を読み込んで UI 上で group として表示
- ユーザーが編集
- 保存時に legacy / form_groups どちらで書くか判定
この一連の流れで state 管理が複雑になり、バグが混入しやすい。

**対策**: 読込時に必ず内部形式（form_groups）へ変換し、保存時に legacy か form_groups かを判定するのではなく、常に form_groups 形式で保存する方針を検討（後方互換性の問題は別途検討）。

### 11. single-open accordion のスクロール位置 (UI崩れ)

**指摘**: Task 3-3 で「single-open accordion」を採用するとあるが、多数の group がある場合の UX が不明。

**リスク**: 展開した group が画面外に飛ぶ、または accordion 開閉時にスクロール位置がリセットされる。

**対策**: 
- `ScrollArea::show_rows` による仮想化
- 展開時にその group へ自動スクロール
- または「上段一覧 + 下段 selected group detail」方式への変更

### 12. migration 通知の実装タイミング (意図しない動作)

**指摘**: Task 1-5 と Task 4-3 で migration 通知が登場するが、どのタイミングで何を表示するかが不明。

**リスク**: 
- 読込時に「legacy 形式です」と表示するのか
- 保存時に「新形式へ移行します」と表示するのか
- 両方か

**対策**: 以下を明確化:
- 読込時: legacy 形式を検出したら badge 表示
- 保存時: legacy → form_groups 変換が発生する場合、確認ダイアログまたは status message

### 13. unknown field 保持のテスト不足 (バグの可能性)

**指摘**: Task 5-3 と Task 6-2 で unknown field 保持を確認するとあるが、テストケースが具体的でない。

**リスク**: `extra_fields` 経由で legacy keys が復活するケースを見逃す可能性。

**対策**: 具体的なテストケースを追加:
```rust
#[test]
fn test_legacy_fields_removed_from_extra_fields() {
    let json = r#"{
        "condition_id": "test",
        "forms": ["a"],
        "forms": ["b"]  // unknown field として extra_fields へ
    }"#;
    // ... deserialize, sanitize, serialize, assert "forms" not in output
}
```

### 14. `not` group の distance UI 制御 (UI崩れ)

**指摘**: Task 4-2 で「`not` group では distance を disabled」とあるが、既存値がある場合どう表示するか。

**リスク**: 既存 JSON に `not` group で `max_token_distance` が設定されている場合、UI で disabled にすると値が見えなくなる、または値が残ったまま保存される不整合。

**対策**: 
- 既存値がある場合は警告表示
- 保存時に `not` group の `max_token_distance` を `None` に補正

### 15. form_groups 編集中の legacy fields 混在防止 (UI崩れ)

**指摘**: Phase 3 で legacy UI を撤去するが、移行期間中に legacy fields と form_groups が混在する可能性。

**リスク**: ユーザーが group 編集中に誤って legacy fields を意識してしまい、どちらが有効か混乱する。

**対策**: 
- 移行期間中は legacy fields を read-only で表示
- または legacy fields を完全に非表示にして form_groups のみ編集可能に

---

## 総合評価

タスクブレイクダウン自体は論理的で網羅的だが、以下の点で実装リスクが高い:

1. **Phase 1 (保存事故防止)** の `normalize_legacy_fields_for_save` 実装が複雑で、バグ混入リスクが高い
2. **extra_fields からの legacy keys 復活** は見落としやすい重大バグ
3. **projection state 管理** は state 数が増え、テストが困難になる可能性

推奨: Phase 1 完了後に必ず JSON round-trip テストを実施し、mixed schema が再発しないことを確認してから Phase 2 以降へ進むこと。
