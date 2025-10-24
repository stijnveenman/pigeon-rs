use ratatui::{
    crossterm::event::KeyCode,
    layout::{Constraint, Direction, Layout},
};

use crate::{
    component::{Component, Tx},
    components::{record_list::RecordList, topic_list::TopicList},
    prompt::Prompt,
    tui_event::TuiEvent,
};

pub struct App {
    pub should_close: bool,
    topic_list: TopicList,
    record_list: RecordList,
    topics_active: bool,
    prompt: Option<Prompt>,
}

impl App {
    pub fn new(tx: Tx) -> Self {
        Self {
            should_close: false,
            topic_list: TopicList::new(tx.clone()),
            record_list: RecordList::new(),
            topics_active: true,
            prompt: None,
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

        if let Some(prompt) = &mut self.prompt {
            prompt.render(f);
        }
    }

    fn event(&mut self, event: TuiEvent) -> Option<TuiEvent> {
        if let Some(prompt) = self.prompt.take() {
            self.prompt = prompt.event(event);
            return None;
        }

        let event = match self.topics_active {
            true => self.topic_list.event(event)?,
            false => self.record_list.event(event)?,
        };

        match event {
            TuiEvent::KeyPress(key) => match key.code {
                KeyCode::Char('q') => self.should_close = true,
                KeyCode::Esc => self.should_close = true,
                KeyCode::Tab => self.topics_active = !self.topics_active,
                _ => {}
            },
            TuiEvent::Prompt(prompt) => {
                self.prompt = Some(prompt);
            }
            _ => {}
        };

        None
    }
}
