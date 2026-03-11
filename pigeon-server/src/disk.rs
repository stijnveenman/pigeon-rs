use std::{
    any,
    collections::HashMap,
    fs::{self, DirEntry},
    path::Path,
};

use anyhow::{Context, bail};

use crate::dur::INDEX_EXTENSION;

fn dir_entry_filename(entry: DirEntry) -> anyhow::Result<String> {
    let path = entry.path();
    Ok(path
        .file_name()
        .context(format!("Failed to get file_name from path {path:?}"))?
        .to_str()
        .context(format!("Failed to convert path to string {path:?}"))?
        .to_string())
}

pub fn read_topics<P: AsRef<Path>>(base_dir: P) -> anyhow::Result<Vec<String>> {
    let mut topics = Vec::new();
    for entry in fs::read_dir(base_dir)? {
        let entry = entry?;

        if !entry.file_type()?.is_dir() {
            bail!("Expected {:?} to be a directory", entry.path());
        }

        topics.push(dir_entry_filename(entry)?);
    }

    Ok(topics)
}

pub fn read_partitions<P: AsRef<Path>>(base_dir: P) -> anyhow::Result<Vec<u64>> {
    let mut partitions = Vec::new();
    for entry in fs::read_dir(base_dir)? {
        let entry = entry?;

        if !entry.file_type()?.is_dir() {
            bail!("Expected {:?} to be a directory", entry.path());
        }

        partitions.push(dir_entry_filename(entry)?.parse()?);
    }

    Ok(partitions)
}

pub fn read_segments<P: AsRef<Path>>(base_dir: P) -> anyhow::Result<Vec<u64>> {
    let mut segments = Vec::new();
    for entry in fs::read_dir(base_dir)? {
        let entry = entry?;

        if !entry.file_type()?.is_file() {
            bail!("Expected {:?} to be a file", entry.path());
        }

        let path = entry.path();

        if path.extension().context("File missing file extension")? != INDEX_EXTENSION {
            continue;
        }

        segments.push(
            path.file_stem()
                .context(format!("Failed to get file_name from path {path:?}"))?
                .to_str()
                .context(format!("Failed to convert path to string {path:?}"))?
                .to_string()
                .parse()?,
        )
    }

    Ok(segments)
}

pub fn read_partition_states<P: AsRef<Path>>(base_dir: P) -> anyhow::Result<Vec<Vec<u64>>> {
    let partitions = read_partitions(&base_dir)?;

    partitions
        .into_iter()
        .map(move |partition| {
            let base_dir = base_dir.as_ref().join(partition.to_string());
            let segments = read_segments(base_dir)?;

            Ok(segments)
        })
        .collect::<Result<Vec<_>, _>>()
}

pub fn read_topic_states<P: AsRef<Path>>(
    base_dir: P,
) -> anyhow::Result<HashMap<String, Vec<Vec<u64>>>> {
    let topics = read_topics(&base_dir)?;

    topics
        .into_iter()
        .map(|topic| {
            let base_dir = base_dir.as_ref().join(&topic);
            let partitions = read_partition_states(base_dir)?;

            Ok((topic, partitions))
        })
        .collect::<Result<HashMap<_, _>, _>>()
}
