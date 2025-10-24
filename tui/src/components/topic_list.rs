use ratatui::{
    crossterm::event::KeyCode,
    style::{Modifier, Style, Stylize},
    widgets::{Block, BorderType, Borders, HighlightSpacing, List, ListItem, ListState},
};

use crate::{
    component::Component,
    form::{Form, QuestionType},
    style::{ACTIVE_BORDER_COLOR, BORDER_STYLE, StylizeIf},
    tui_event::TuiEvent,
};

pub struct TopicList {
    topics: Vec<String>,
    list_state: ListState,
}

impl TopicList {
    pub fn new() -> Self {
        Self {
            topics: ["__metadata", "foo", "bar"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            list_state: ListState::default().with_selected(Some(0)),
        }
    }
}

impl Component for TopicList {
    fn event(
        &mut self,
        event: crate::tui_event::TuiEvent,
        tx: crate::component::Tx,
    ) -> Option<crate::tui_event::TuiEvent> {
        match event {
            TuiEvent::AddTopic(topic) => self.topics.push(topic),
            TuiEvent::KeyPress(key) => match key.code {
                KeyCode::Char('j') => self.list_state.select_next(),
                KeyCode::Char('k') => self.list_state.select_previous(),
                KeyCode::Char('g') => self.list_state.select_first(),
                KeyCode::Char('G') => self.list_state.select_last(),
                KeyCode::Char('a') => {
                    tokio::spawn(async move {
                        let Ok(mut result) = Form::new()
                            .title("Add new topic")
                            .push("Name", QuestionType::String, true)
                            .push("Partitions", QuestionType::Integer, false)
                            .show(tx.clone())
                            .await
                        else {
                            return;
                        };

                        let topic = result.get("Name").unwrap();
                        tx.send(TuiEvent::AddTopic(topic)).unwrap();
                    });
                }
                _ => return Some(event),
            },
            _ => return Some(event),
        };

        None
    }

    fn render(&mut self, f: &mut ratatui::Frame, rect: ratatui::prelude::Rect, active: bool) {
        let block = Block::new()
            .borders(Borders::ALL)
            .border_type(BorderType::Double)
            .border_style(BORDER_STYLE.fg_if(ACTIVE_BORDER_COLOR, active))
            .title("Topics");

        let inner = block.inner(rect);
        f.render_widget(block, rect);
        let rect = inner;

        let items: Vec<ListItem> = self
            .topics
            .iter()
            .map(|topic| ListItem::new(topic.as_str()))
            .collect();

        let list = List::new(items)
            .highlight_symbol(">")
            .highlight_style(Style::new().on_blue().black().add_modifier(Modifier::BOLD))
            .highlight_spacing(HighlightSpacing::Always);

        f.render_stateful_widget(list, rect, &mut self.list_state);
    }
}
