use std::{io, sync::Mutex, time::Duration};

use ratatui::crossterm::event::{self, KeyEvent, KeyEventKind};

use crate::component::Component;

pub enum TuiEvent {
    KeyPress(KeyEvent),
    Popup(Option<Mutex<Box<dyn Component + Send>>>),
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

    pub fn popup<T: Component + Send + 'static>(popup: T) -> TuiEvent {
        TuiEvent::Popup(Some(Mutex::new(Box::new(popup))))
    }
}
