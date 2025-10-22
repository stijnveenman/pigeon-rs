use ratatui::{
    crossterm::event::KeyCode,
    layout::{Alignment, Constraint, Direction, Layout},
};

use crate::{
    component::{Component, Tx},
    components::{record_list::RecordList, topic_list::TopicList},
    style::BORDER_STYLE,
    tui_event::TuiEvent,
    widgets::popup::Popup,
};

pub struct App {
    pub should_close: bool,
    topic_list: TopicList,
    record_list: RecordList,
}

impl App {
    pub fn new() -> Self {
        Self {
            should_close: false,
            topic_list: TopicList::new(true),
            record_list: RecordList::new(false),
        }
    }
}

impl Component for App {
    fn render(&mut self, f: &mut ratatui::Frame, rect: ratatui::prelude::Rect) {
        let [topics, records] = Layout::default()
            .direction(Direction::Horizontal)
            .constraints(vec![Constraint::Percentage(25), Constraint::Percentage(75)])
            .areas(rect);

        self.topic_list.render(f, topics);
        self.record_list.render(f, records);

        let popup = Popup::new().title("Popup");
        f.render_widget(popup.clone(), rect);
        f.render_widget("lorom", popup.inner(rect));
    }

    fn event(&mut self, event: TuiEvent, tx: Tx) -> Option<TuiEvent> {
        let event = self.topic_list.event(event, tx)?;

        match event {
            TuiEvent::KeyPress(key) => match key.code {
                KeyCode::Char('q') => self.should_close = true,
                KeyCode::Esc => self.should_close = true,
                KeyCode::Tab => {
                    self.topic_list.is_active = !self.topic_list.is_active;
                    self.record_list.is_active = !self.record_list.is_active;
                }
                _ => {}
            },
        };

        None
    }
}
