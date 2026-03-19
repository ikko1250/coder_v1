use egui;

pub(crate) fn ime_safe_singleline<'t>(text: &'t mut dyn egui::TextBuffer) -> egui::TextEdit<'t> {
    egui::TextEdit::singleline(text).return_key(None)
}

pub(crate) fn ime_safe_multiline<'t>(text: &'t mut dyn egui::TextBuffer) -> egui::TextEdit<'t> {
    egui::TextEdit::multiline(text).return_key(egui::KeyboardShortcut::new(
        egui::Modifiers::SHIFT,
        egui::Key::Enter,
    ))
}
