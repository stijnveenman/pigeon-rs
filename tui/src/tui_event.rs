use std::{io, time::Duration};

use ratatui::crossterm::event::{self, KeyEvent, KeyEventKind};

pub enum TuiEvent {
    KeyPress(KeyEvent),
    Close,
}

impl TuiEvent {
    pub fn read(tick_rate: Duration) -> io::Result<Option<TuiEvent>> {
        if !event::poll(tick_rate)? {
            return Ok(None);
        }

        let event = match event::read()? {
            event::Event::Key(key) if key.kind == KeyEventKind::Press => TuiEvent::KeyPress(key),
            _ => return Ok(None),
        };

        Ok(Some(event))
    }
}
