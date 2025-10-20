use std::io;

use ratatui::crossterm::event::{self, KeyEvent, KeyEventKind};

pub enum Event {
    KeyPress(KeyEvent),
}

impl Event {
    pub fn read() -> io::Result<Option<Event>> {
        let event = match event::read()? {
            event::Event::Key(key) if key.kind == KeyEventKind::Press => Some(Event::KeyPress(key)),
            _ => None,
        };

        Ok(event)
    }
}
