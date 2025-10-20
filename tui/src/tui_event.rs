use std::io;

use ratatui::crossterm::event::{self, KeyEvent, KeyEventKind};

pub enum TuiEvent {
    KeyPress(KeyEvent),
}

impl TuiEvent {
    pub fn read() -> io::Result<Option<TuiEvent>> {
        let event = match event::read()? {
            event::Event::Key(key) if key.kind == KeyEventKind::Press => {
                Some(TuiEvent::KeyPress(key))
            }
            _ => None,
        };

        Ok(event)
    }
}
