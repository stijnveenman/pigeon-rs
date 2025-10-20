use crate::component::Component;

#[derive(Default)]
pub struct App {
    pub should_close: bool,
}

impl Component for App {
    fn render(&self, f: &mut ratatui::Frame, rect: ratatui::prelude::Rect) {}

    fn handle_event(&mut self, event: crate::event::Event) -> bool {
        self.should_close = true;

        true
    }
}
