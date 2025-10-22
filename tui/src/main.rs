mod app;
mod component;
mod components;
mod style;
mod tui_event;

use std::{io, time::Duration};

use anyhow::Result;
use app::App;
use component::Component;
use ratatui::{
    Terminal,
    crossterm::{
        event::{DisableMouseCapture, EnableMouseCapture},
        execute,
        terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
    },
    prelude::CrosstermBackend,
};
use tokio::sync::mpsc;
use tui_event::TuiEvent;

#[tokio::main]
// This is far from pretty, but it's mostly async wiring
async fn main() -> Result<()> {
    enable_raw_mode()?;
    let mut stderr = io::stderr();
    execute!(stderr, EnterAlternateScreen, EnableMouseCapture)?;

    let backend = CrosstermBackend::new(stderr);
    let mut terminal = Terminal::new(backend)?;

    let (tx, mut rx) = mpsc::unbounded_channel();
    let (key_tx, mut key_rx) = mpsc::unbounded_channel();

    let key_tx2 = key_tx.clone();
    let key_task = tokio::spawn(async move {
        let tick_rate = Duration::from_millis(250);
        loop {
            let event = TuiEvent::read(tick_rate).unwrap();
            if key_tx.send(event).is_err() {
                break;
            }
        }
    });

    let rx_task = tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            if key_tx2.send(Some(event)).is_err() {
                break;
            }
        }
    });

    let mut app = App::new();
    while !app.should_close {
        terminal.draw(|f| app.render(f, f.area()))?;

        let Some(received) = key_rx.recv().await else {
            // Channel closed
            break;
        };

        if let Some(event) = received {
            app.event(event, tx.clone());
        } else {
            app.tick(tx.clone());
        }
    }

    key_task.abort();
    rx_task.abort();

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    Ok(())
}
