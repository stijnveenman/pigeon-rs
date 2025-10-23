use std::sync::Mutex;

use ratatui::{
    crossterm::event::KeyCode,
    layout::{Constraint, Direction, Layout},
};

use crate::{
    component::{Component, Tx},
    components::{record_list::RecordList, topic_list::TopicList},
    form::FormPopup,
    tui_event::TuiEvent,
};

pub struct App {
    pub should_close: bool,
    topic_list: TopicList,
    record_list: RecordList,
    topics_active: bool,
    form: Option<FormPopup>,
}

impl App {
    pub fn new() -> Self {
        Self {
            should_close: false,
            topic_list: TopicList::new(),
            record_list: RecordList::new(),
            topics_active: true,
            form: None,
        }
    }
}

impl Component for App {
    fn render(&mut self, f: &mut ratatui::Frame, rect: ratatui::prelude::Rect, _active: bool) {
        let [topics, records] = Layout::default()
            .direction(Direction::Horizontal)
            .constraints(vec![Constraint::Percentage(25), Constraint::Percentage(75)])
            .areas(rect);

        self.topic_list.render(f, topics, self.topics_active);
        self.record_list.render(f, records, !self.topics_active);

        if let Some(form) = &mut self.form {
            form.render(f, rect, true);
        }
    }

    fn event(&mut self, event: TuiEvent, tx: Tx) -> Option<TuiEvent> {
        if let Some(form) = self.form.take() {
            self.form = form.event(event);
            return None;
        }

        let event = match self.topics_active {
            true => self.topic_list.event(event, tx)?,
            false => self.record_list.event(event, tx)?,
        };

        match event {
            TuiEvent::KeyPress(key) => match key.code {
                KeyCode::Char('q') => self.should_close = true,
                KeyCode::Esc => self.should_close = true,
                KeyCode::Tab => self.topics_active = !self.topics_active,
                _ => {}
            },
            TuiEvent::Form(form) => {
                self.form = Some(form);
            }
        };

        None
    }
}
