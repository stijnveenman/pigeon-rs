use std::{collections::BTreeMap, time::Duration};

use client::http_client::HttpClient;
use ratatui::{
    crossterm::event::KeyCode,
    layout::Constraint,
    style::{Modifier, Style, Stylize},
    widgets::{Block, BorderType, Borders, HighlightSpacing, List, ListItem, ListState},
};
use shared::{consts::DEFAULT_PORT, state::topic_state::TopicState};
use tokio::{task::JoinHandle, time::sleep};

use crate::{
    component::{Component, Tx},
    prompt::{Input, Prompt, PromptType},
    style::{ACTIVE_BORDER_COLOR, BORDER_STYLE, StylizeIf},
    tui_event::TuiEvent,
};

pub struct TopicList {
    topics: BTreeMap<u64, TopicState>,
    list_state: ListState,
    tx: Tx,
    refresh_task: JoinHandle<()>,
}

impl TopicList {
    pub fn new(tx: Tx) -> Self {
        Self {
            topics: BTreeMap::new(),
            list_state: ListState::default().with_selected(Some(0)),
            tx: tx.clone(),
            refresh_task: tokio::spawn(async move {
                let client = HttpClient::new(format!("http://127.0.0.1:{}", DEFAULT_PORT)).unwrap();

                loop {
                    let topics = client.get_topics().await.unwrap();

                    tx.send(TuiEvent::TopicList(topics.into_iter().collect()))
                        .unwrap();

                    sleep(Duration::from_secs(1)).await;
                }
            }),
        }
    }

    fn send_selected(&self) -> Option<()> {
        let idx = self.list_state.selected()?;
        let topic = self.topics.values().nth(idx)?;
        self.tx.send(TuiEvent::SelectTopic(topic.clone())).unwrap();

        Some(())
    }
}

impl Component for TopicList {
    fn event(&mut self, event: crate::tui_event::TuiEvent) -> Option<crate::tui_event::TuiEvent> {
        match event {
            TuiEvent::AddTopic(topic) => {
                self.topics.insert(topic.topic_id, topic);
            }
            TuiEvent::RemoveTopic(topic) => {
                self.topics.remove(&topic);
                return Some(event);
            }
            TuiEvent::TopicList(topics) => {
                self.topics = topics;
                if self.list_state.selected().is_none() {
                    self.list_state.select_next();
                    self.send_selected();
                }
            }
            TuiEvent::KeyPress(key) => match key.code {
                KeyCode::Char('j') => {
                    self.list_state.select_next();
                    self.send_selected();
                }
                KeyCode::Char('k') => {
                    self.list_state.select_previous();
                    self.send_selected();
                }
                KeyCode::Char('g') => {
                    self.list_state.select_first();
                    self.send_selected();
                }
                KeyCode::Char('G') => {
                    self.list_state.select_last();
                    self.send_selected();
                }
                KeyCode::Char('i') => {
                    let idx = self.list_state.selected()?;
                    let topic = self.topics.values().nth(idx)?;

                    let partitions: String =
                        topic.partitions.iter().fold(String::new(), |prev, p| {
                            format!(
                                "{}Partition {}: {}\n",
                                prev, p.partition_id, p.current_offset
                            )
                        });

                    Prompt::new("Topic Info")
                        .prompt_type(PromptType::Info)
                        .width(Constraint::Length(30))
                        .title(format!("Topic: {}", topic.name))
                        .paragraph(format!("Id: {}", topic.topic_id))
                        .paragraph(format!("Name: {}", topic.name))
                        .paragraph(format!("Partitions: {}", topic.partitions.len()))
                        .padding(1)
                        .paragraph("Offsets")
                        .paragraph(partitions)
                        .show(self.tx.clone());
                }
                KeyCode::Char('d') => {
                    let idx = self.list_state.selected()?;
                    let topic = self.topics.values().nth(idx)?;
                    let name = topic.name.clone();
                    let id = topic.topic_id;

                    let tx = self.tx.clone();
                    tokio::spawn(async move {
                        if Prompt::new("Delete topic")
                            .paragraph(format!("Are you sure you want to delete topic: {}", name))
                            .show(tx.clone())
                            .await
                            .is_err()
                        {
                            return;
                        }

                        let client =
                            HttpClient::new(format!("http://127.0.0.1:{}", DEFAULT_PORT)).unwrap();

                        if let Err(err) = client.delete_topic(&name).await {
                            Prompt::error("Delete topic failed", err.to_string()).show(tx);
                        } else {
                            tx.send(TuiEvent::RemoveTopic(id)).unwrap();
                        }
                    });
                }
                KeyCode::Char('a') => {
                    let tx = self.tx.clone();
                    tokio::spawn(async move {
                        let Ok(result) = Prompt::new("Create topic")
                            .title("Add new topic")
                            .input(Input::string("Name").required())
                            .input(Input::integer("Partitions"))
                            .show(tx.clone())
                            .await
                        else {
                            return;
                        };

                        let topic: String = result.get("Name").unwrap();
                        let partitions = result.get("Partitions").ok();

                        let client =
                            HttpClient::new(format!("http://127.0.0.1:{}", DEFAULT_PORT)).unwrap();

                        match client.create_topic(&topic, partitions).await {
                            Ok(topic) => tx.send(TuiEvent::AddTopic(topic)).unwrap(),
                            Err(err) => {
                                Prompt::error("Create topic failed", err.to_string()).show(tx);
                            }
                        }
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
            .values()
            .map(|topic| ListItem::new(topic.name.clone()))
            .collect();

        let list = List::new(items)
            .highlight_symbol(">")
            .highlight_style(Style::new().on_blue().black().add_modifier(Modifier::BOLD))
            .highlight_spacing(HighlightSpacing::Always);

        f.render_stateful_widget(list, rect, &mut self.list_state);
    }
}

impl Drop for TopicList {
    fn drop(&mut self) {
        self.refresh_task.abort();
    }
}
