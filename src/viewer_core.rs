//! 条例分析ビューアのドメインコア（P2）。
//!
//! **egui / eframe に依存しない**型・状態のみを置く。UI アダプタ（[`crate::app`]）から段階的に移行する。
//!
//! - **P2-01**: 本モジュールと空に近い [`ViewerCoreState`] のみ（コンパイル可能な足場）。
//! - **P2-02 以降**: レコード・フィルタ・選択などをここへ移す想定。

/// コア状態の器（P2-01: プレースホルダ。P2-02 以降でフィールドを追加）。
///
/// 現状 `App` 未接続のため非 test ビルドでは未使用。P2-02 でホストが保持するまで
/// `dead_code` を許容する。
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct ViewerCoreState {}

#[cfg(test)]
mod tests {
    use super::ViewerCoreState;

    #[test]
    fn viewer_core_state_defaults() {
        let _ = ViewerCoreState::default();
    }
}
