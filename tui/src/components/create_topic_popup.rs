use crate::{component::Component, widgets::popup::Popup};

pub struct CreateTopicPopup {}

impl Component for CreateTopicPopup {
    fn render(&mut self, f: &mut ratatui::Frame, rect: ratatui::prelude::Rect, _active: bool) {
        let popup = Popup::new().title("Popup");
        f.render_widget(popup.clone(), rect);
        f.render_widget("lorom", popup.inner(rect));
    }
}
