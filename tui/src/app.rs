use crate::{component::Component, tui_event::TuiEvent};

#[derive(Default)]
pub struct App {
    pub should_close: bool,
}

impl Component for App {
    fn render(&self, f: &mut ratatui::Frame, _rect: ratatui::prelude::Rect) {}

    fn event(&mut self, _event: TuiEvent) -> Option<TuiEvent> {
        self.should_close = true;

        None
    }
}
