use ratatui::{Frame, layout::Rect};

use crate::event::Event;

pub trait Component {
    /// Handle a specific event, should return true to stop 'propogation'; ie, if the Component did
    /// something with the event.
    ///
    /// Examples might be handling characters, which should prevent a global 'q' char from exiting
    /// the application
    #[allow(unused_variables)]
    fn handle_event(&mut self, event: Event) -> bool {
        false
    }

    /// Generic update which is called repeatedly
    fn update(&mut self) {}

    /// Render call, should render the Component into rect for this frame
    fn render(&self, f: &mut Frame, rect: Rect);
}
