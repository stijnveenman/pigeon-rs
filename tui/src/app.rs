use std::time::Duration;

use crate::{
    component::{Component, Tx},
    tui_event::TuiEvent,
};

#[derive(Default)]
pub struct App {
    pub should_close: bool,
}

impl Component for App {
    fn render(&self, f: &mut ratatui::Frame, _rect: ratatui::prelude::Rect) {}

    fn event(&mut self, _event: TuiEvent, tx: Tx) -> Option<TuiEvent> {
        let tx = tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            tx.send(Some(TuiEvent::Close))
        });

        match _event {
            TuiEvent::Close => {
                self.should_close = true;
                None
            }
            e => Some(e),
        }
    }
}
