use ratatui::{
    crossterm::event::KeyCode,
    layout::{Constraint, Direction, Layout},
    style::{Style, Stylize},
    widgets::{Block, BorderType, Borders, Paragraph},
};

use crate::{
    component::{Component, Tx},
    components::topic_list::TopicList,
    tui_event::TuiEvent,
};

pub struct App {
    pub should_close: bool,
    topic_list: TopicList,
}

impl App {
    pub fn new() -> Self {
        Self {
            should_close: false,
            topic_list: TopicList::new(),
        }
    }
}

impl Component for App {
    fn render(&mut self, f: &mut ratatui::Frame, rect: ratatui::prelude::Rect) {
        let [topics, records] = Layout::default()
            .direction(Direction::Horizontal)
            .constraints(vec![Constraint::Percentage(25), Constraint::Percentage(75)])
            .areas(rect);

        let block = Block::new()
            .borders(Borders::ALL)
            .border_type(BorderType::Double)
            .border_style(Style::new().blue().bold().italic());

        f.render_widget(block.clone().title("Topics"), topics);
        f.render_widget(block.clone().title("Records"), records);

        self.topic_list.render(f, block.inner(topics));

        let p = Paragraph::new("Lorum ipsum");
        f.render_widget(p.clone(), block.inner(records));
    }

    fn event(&mut self, event: TuiEvent, tx: Tx) -> Option<TuiEvent> {
        let event = self.topic_list.event(event, tx)?;

        match event {
            TuiEvent::KeyPress(key) => match key.code {
                KeyCode::Char('q') => self.should_close = true,
                KeyCode::Esc => self.should_close = true,
                _ => {}
            },
        };

        None
    }
}
