use ratatui::widgets::{HighlightSpacing, List, ListItem, ListState};

use crate::component::Component;

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
    fn render(&self, f: &mut ratatui::Frame, rect: ratatui::prelude::Rect) {
        let items: Vec<ListItem> = self
            .topics
            .iter()
            .map(|topic| ListItem::new(topic.as_str()))
            .collect();

        let list = List::new(items)
            .highlight_symbol(">")
            .highlight_spacing(HighlightSpacing::Always);

        // TODO: make render mutable
        let mut new_state = self.list_state.clone();
        f.render_stateful_widget(list, rect, &mut new_state);
        assert_eq!(self.list_state, new_state);
    }
}
