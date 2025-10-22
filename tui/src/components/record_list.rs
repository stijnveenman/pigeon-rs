use ratatui::widgets::{Block, BorderType, Borders, Paragraph};

use crate::{
    component::Component,
    style::{ACTIVE_BORDER_COLOR, BORDER_STYLE, StylizeIf},
};

pub struct RecordList {}

impl RecordList {
    pub fn new() -> Self {
        Self {}
    }
}

impl Component for RecordList {
    fn render(&mut self, f: &mut ratatui::Frame, rect: ratatui::prelude::Rect, active: bool) {
        let block = Block::new()
            .borders(Borders::ALL)
            .border_type(BorderType::Double)
            .border_style(BORDER_STYLE.fg_if(ACTIVE_BORDER_COLOR, active))
            .title("Records");

        let inner = block.inner(rect);
        f.render_widget(block, rect);
        let rect = inner;

        let p = Paragraph::new("Lorum ipsum");
        f.render_widget(p, rect);
    }
}
