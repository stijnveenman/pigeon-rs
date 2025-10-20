use ratatui::{Frame, layout::Rect};

use crate::tui_event::TuiEvent;

pub trait Component {
    /// Handle a specific TuiEvent, should return None if the event is handled. If the Component
    /// did nothing with the event, it should be returned back
    ///
    /// Examples of not handling an event might be if no text field is selected, 'q' should close
    /// the main application. But this is only known once the event is handled by a child Component
    fn event(&mut self, event: TuiEvent) -> Option<TuiEvent> {
        Some(event)
    }

    /// Generic tick is called if no events have been received for a while
    fn tick(&mut self) {}

    /// Render call, should render the Component into rect for this frame
    fn render(&self, f: &mut Frame, rect: Rect);
}
