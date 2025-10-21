use ratatui::widgets::{List, ListItem};

use crate::component::Component;

pub struct TopicList {
    topics: Vec<String>,
}

impl TopicList {
    pub fn new() -> Self {
        Self {
            topics: ["__metadata", "foo", "bar"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
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

        let list = List::new(items);

        f.render_widget(list, rect);
    }
}
