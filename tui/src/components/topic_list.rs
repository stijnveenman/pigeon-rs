use ratatui::{
    crossterm::event::KeyCode,
    layout::Alignment,
    style::{Modifier, Style, Stylize},
    widgets::{Block, BorderType, Borders, HighlightSpacing, List, ListItem, ListState},
};

use crate::{
    component::Component,
    style::{ACTIVE_BORDER_COLOR, BORDER_STYLE, StylizeIf},
    tui_event::TuiEvent,
    widgets::popup::Popup,
};

pub struct TopicList {
    pub is_active: bool,
    topics: Vec<String>,
    list_state: ListState,
}

impl TopicList {
    pub fn new(is_active: bool) -> Self {
        Self {
            is_active,
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
        _tx: crate::component::Tx,
    ) -> Option<crate::tui_event::TuiEvent> {
        if !self.is_active {
            return Some(event);
        };

        match event {
            TuiEvent::KeyPress(key) => match key.code {
                KeyCode::Char('j') => self.list_state.select_next(),
                KeyCode::Char('k') => self.list_state.select_previous(),
                KeyCode::Char('g') => self.list_state.select_first(),
                KeyCode::Char('G') => self.list_state.select_last(),
                _ => return Some(event),
            },
        };

        None
    }

    fn render(&mut self, f: &mut ratatui::Frame, rect: ratatui::prelude::Rect) {
        let block = Block::new()
            .borders(Borders::ALL)
            .border_type(BorderType::Double)
            .border_style(BORDER_STYLE.fg_if(ACTIVE_BORDER_COLOR, self.is_active))
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

        let popup = Popup::new(20, 50)
            .border_style(BORDER_STYLE)
            .horizontal_alignment(Alignment::Right)
            .vertical_alignment(Alignment::Right)
            .title("Popup");
        f.render_widget(popup.clone(), rect);
        f.render_widget("lorom", popup.inner(rect));
    }
}
