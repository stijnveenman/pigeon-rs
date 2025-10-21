use ratatui::{Frame, layout::Rect};
use tokio::sync::mpsc;

use crate::tui_event::TuiEvent;

pub type Tx = mpsc::UnboundedSender<TuiEvent>;

pub trait Component {
    /// Handle a specific TuiEvent, should return None if the event is handled. If the Component
    /// did nothing with the event, it should be returned back
    ///
    /// Examples of not handling an event might be if no text field is selected, 'q' should close
    /// the main application. But this is only known once the event is handled by a child Component
    #[allow(unused_variables)]
    fn event(&mut self, event: TuiEvent, tx: Tx) -> Option<TuiEvent> {
        Some(event)
    }

    /// Generic tick is called if no events have been received for a while
    #[allow(unused_variables)]
    fn tick(&mut self, tx: Tx) {}

    /// Render call, should render the Component into rect for this frame
    fn render(&self, f: &mut Frame, rect: Rect);
}
