use ratatui::widgets::{Block, BorderType, Borders, Paragraph};
use shared::state::topic_state::TopicState;

use crate::{
    component::Component,
    style::{ACTIVE_BORDER_COLOR, BORDER_STYLE, StylizeIf},
    tui_event::TuiEvent,
};

pub struct RecordList {
    topic: Option<TopicState>,
}

impl RecordList {
    pub fn new() -> Self {
        Self { topic: None }
    }
}

impl Component for RecordList {
    fn event(&mut self, event: TuiEvent) -> Option<TuiEvent> {
        match event {
            TuiEvent::SelectTopic(topic) => self.topic = Some(topic),
            e => return Some(e),
        };

        None
    }

    fn render(&mut self, f: &mut ratatui::Frame, rect: ratatui::prelude::Rect, active: bool) {
        let title = match &self.topic {
            Some(topic) => format!("{} > Records", topic.name),
            None => "No topic selected".to_string(),
        };

        let block = Block::new()
            .borders(Borders::ALL)
            .border_type(BorderType::Double)
            .border_style(BORDER_STYLE.fg_if(ACTIVE_BORDER_COLOR, active))
            .title(title);

        let inner = block.inner(rect);
        f.render_widget(block, rect);
        let rect = inner;

        let p = Paragraph::new("Lorum ipsum");
        f.render_widget(p, rect);
    }
}
