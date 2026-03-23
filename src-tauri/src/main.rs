#[path = "../../src/ipc_dto.rs"]
mod ipc_dto;

use eframe::egui;

fn main() {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title("P5-02 Tauri pilot mock")
            .with_inner_size([760.0, 520.0]),
        ..Default::default()
    };

    eframe::run_native(
        "P5-02 Tauri pilot mock",
        options,
        Box::new(|_cc| Ok(Box::new(PilotApp::default()))),
    )
    .unwrap();
}

#[derive(Default)]
struct PilotApp {
    invoke_output: String,
}

impl PilotApp {
    fn invoke_get_ipc_dto_snapshot(&mut self) {
        self.invoke_output = match ipc_dto::run_ipc_dto_self_check() {
            Ok(output) => output,
            Err(error) => format!("invoke failed: {error}"),
        };
    }
}

impl eframe::App for PilotApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("P5-02: minimal front + invoke");
            ui.label("ボタン押下で P4 DTO の自己検証結果を表示します。");
            ui.add_space(8.0);
            if ui.button("invoke: getIpcDtoSnapshot").clicked() {
                self.invoke_get_ipc_dto_snapshot();
            }
            ui.add_space(8.0);
            ui.separator();
            ui.add(
                egui::TextEdit::multiline(&mut self.invoke_output)
                    .desired_width(f32::INFINITY)
                    .desired_rows(20),
            );
        });
    }
}
