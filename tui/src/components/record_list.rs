use std::collections::HashMap;

use client::http_client::HttpClient;
use ratatui::widgets::{Block, BorderType, Borders, Paragraph};
use shared::{
    commands::fetch_command::{FetchCommand, FetchPartitionCommand, FetchTopicCommand},
    consts::DEFAULT_PORT,
    data::{encoding::Encoding, identifier::Identifier, offset_selection::OffsetSelection},
    response::record_response::RecordResponse,
    state::topic_state::TopicState,
};
use tokio::task::JoinHandle;

use crate::{
    component::{Component, Tx},
    prompt::Prompt,
    style::{ACTIVE_BORDER_COLOR, BORDER_STYLE, StylizeIf},
    tui_event::TuiEvent,
};

pub struct RecordList {
    topic: Option<TopicState>,
    update_handle: Option<JoinHandle<()>>,
    record_history_count: u64,
    timeout_ms: u64,
    min_bytes: usize,
    tx: Tx,
    records: Vec<RecordResponse>,
}

impl RecordList {
    pub fn new(tx: Tx) -> Self {
        Self {
            topic: None,
            update_handle: None,
            timeout_ms: 1000,
            min_bytes: 1,
            record_history_count: 10,
            tx,
            records: vec![],
        }
    }

    fn select_topic(&mut self, topic: TopicState) {
        if self.topic.as_ref().is_some_and(|t| t == &topic) {
            return;
        }

        if let Some(handle) = self.update_handle.take() {
            handle.abort();
        }

        self.topic = Some(topic);
        self.records = vec![];
        self.update_handle = self.update_task();
    }

    fn update_task(&self) -> Option<JoinHandle<()>> {
        let Some(topic) = &self.topic else {
            return None;
        };

        let topic_id = topic.topic_id;
        let mut partition_offsets: HashMap<_, _> = topic
            .partitions
            .iter()
            .map(|p| {
                (
                    p.partition_id,
                    p.current_offset.saturating_sub(self.record_history_count),
                )
            })
            .collect();

        let client = HttpClient::new(format!("http://127.0.0.1:{}", DEFAULT_PORT)).unwrap();
        let timeout_ms = self.timeout_ms;
        let min_bytes = self.min_bytes;
        let tx = self.tx.clone();

        let handle = tokio::spawn(async move {
            loop {
                let response = client
                    .fetch(FetchCommand {
                        encoding: Encoding::Utf8,
                        topics: vec![FetchTopicCommand {
                            identifier: Identifier::Id(topic_id),
                            partitions: partition_offsets
                                .iter()
                                .map(|p| FetchPartitionCommand {
                                    id: *p.0,
                                    offset: OffsetSelection::From(*p.1),
                                })
                                .collect(),
                        }],
                        timeout_ms,
                        min_bytes,
                    })
                    .await;

                let response = match response {
                    Ok(r) => r,
                    Err(e) => {
                        tx.send(TuiEvent::Prompt(Prompt::error(e.to_string())))
                            .unwrap();
                        return;
                    }
                };

                for record in response.records {
                    let partition_offset = partition_offsets
                        .get(&record.partition_id)
                        .cloned()
                        .unwrap_or_default()
                        .max(record.offset + 1);
                    partition_offsets.insert(record.partition_id, partition_offset);

                    tx.send(TuiEvent::Record(record)).unwrap();
                }
            }
        });

        Some(handle)
    }
}

impl Component for RecordList {
    fn event(&mut self, event: TuiEvent) -> Option<TuiEvent> {
        match event {
            TuiEvent::SelectTopic(topic) => self.select_topic(topic),
            TuiEvent::Record(record) => self.records.push(record),
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

        let p = Paragraph::new(format!("Records: {}", self.records.len()));
        f.render_widget(p, rect);
    }
}
