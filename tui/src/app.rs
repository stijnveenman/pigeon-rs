use crate::{component::Component, tui_event::TuiEvent};

#[derive(Default)]
pub struct App {
    pub should_close: bool,
}

impl Component for App {
    fn render(&self, f: &mut ratatui::Frame, _rect: ratatui::prelude::Rect) {}

    fn handle_event(&mut self, _event: TuiEvent) -> bool {
        self.should_close = true;

        true
    }
}
