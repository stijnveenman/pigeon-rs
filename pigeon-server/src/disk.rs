use std::{
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
