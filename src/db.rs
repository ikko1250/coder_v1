#![allow(dead_code)]

use crate::model::{DbParagraph, DbParagraphContext};
use rusqlite::{Connection, OpenFlags, Row};
use std::env;
use std::path::{Path, PathBuf};

const DEFAULT_DB_RELATIVE_PATH: &str = "asset/ordinance_analysis5.db";
pub(crate) const DEFAULT_CONTEXT_RADIUS: i64 = 2;

pub(crate) fn resolve_default_db_path() -> PathBuf {
    let relative_path = PathBuf::from(DEFAULT_DB_RELATIVE_PATH);
    if relative_path.is_file() {
        return relative_path;
    }

    if let Ok(exe_path) = env::current_exe() {
        for ancestor in exe_path.ancestors().skip(1).take(4) {
            let candidate = ancestor.join(DEFAULT_DB_RELATIVE_PATH);
            if candidate.is_file() {
                return candidate;
            }
        }
    }

    relative_path
}

pub(crate) fn fetch_paragraph_context(
    db_path: &Path,
    paragraph_id: i64,
) -> Result<DbParagraphContext, String> {
    let connection = open_read_only_connection(db_path)?;
    let center = fetch_center_paragraph(&connection, paragraph_id)?;
    let paragraphs = fetch_context_paragraphs_with_connection(
        &connection,
        center.document_id,
        center.paragraph_no,
        DEFAULT_CONTEXT_RADIUS,
    )?;

    if !paragraphs.iter().any(|paragraph| paragraph.paragraph_id == center.paragraph_id) {
        return Err(format!(
            "中心段落が前後コンテキストに含まれていません: paragraph_id={}",
            center.paragraph_id
        ));
    }

    Ok(DbParagraphContext { center, paragraphs })
}

pub(crate) fn fetch_paragraph_context_by_location(
    db_path: &Path,
    document_id: i64,
    paragraph_no: i64,
) -> Result<DbParagraphContext, String> {
    let connection = open_read_only_connection(db_path)?;
    let paragraphs = fetch_context_paragraphs_with_connection(
        &connection,
        document_id,
        paragraph_no,
        DEFAULT_CONTEXT_RADIUS,
    )?;

    let center = paragraphs
        .iter()
        .find(|paragraph| paragraph.paragraph_no == paragraph_no)
        .cloned()
        .ok_or_else(|| {
            format!(
                "document_id={} / paragraph_no={} の中心段落取得に失敗しました",
                document_id, paragraph_no
            )
        })?;

    Ok(DbParagraphContext { center, paragraphs })
}

pub(crate) fn fetch_context_paragraphs(
    db_path: &Path,
    document_id: i64,
    center_paragraph_no: i64,
    radius: i64,
) -> Result<Vec<DbParagraph>, String> {
    let connection = open_read_only_connection(db_path)?;
    fetch_context_paragraphs_with_connection(&connection, document_id, center_paragraph_no, radius)
}

fn fetch_context_paragraphs_with_connection(
    connection: &Connection,
    document_id: i64,
    center_paragraph_no: i64,
    radius: i64,
) -> Result<Vec<DbParagraph>, String> {
    let start_paragraph_no = (center_paragraph_no - radius).max(1);
    let end_paragraph_no = center_paragraph_no + radius;
    let mut statement = connection
        .prepare(
            "SELECT paragraph_id, document_id, paragraph_no, paragraph_text
             FROM analysis_paragraphs
             WHERE document_id = ?1
               AND paragraph_no BETWEEN ?2 AND ?3
             ORDER BY paragraph_no ASC",
        )
        .map_err(|error| format!("前後段落取得用クエリの準備に失敗しました: {error}"))?;

    let rows = statement
        .query_map(
            [document_id, start_paragraph_no, end_paragraph_no],
            read_db_paragraph,
        )
        .map_err(|error| format!("前後段落の取得に失敗しました: {error}"))?;

    let mut paragraphs = Vec::new();
    for row in rows {
        paragraphs.push(row.map_err(|error| format!("DB 段落の読み取りに失敗しました: {error}"))?);
    }

    Ok(paragraphs)
}

fn open_read_only_connection(db_path: &Path) -> Result<Connection, String> {
    if !db_path.is_file() {
        return Err(format!(
            "DB ファイルが見つかりません: {}",
            db_path.display()
        ));
    }

    Connection::open_with_flags(db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|error| format!("DB を読み取り専用で開けませんでした: {error}"))
}

fn fetch_center_paragraph(connection: &Connection, paragraph_id: i64) -> Result<DbParagraph, String> {
    let mut statement = connection
        .prepare(
            "SELECT paragraph_id, document_id, paragraph_no, paragraph_text
             FROM analysis_paragraphs
             WHERE paragraph_id = ?1",
        )
        .map_err(|error| format!("中心段落取得用クエリの準備に失敗しました: {error}"))?;

    statement
        .query_row([paragraph_id], read_db_paragraph)
        .map_err(|error| format!("paragraph_id={} の中心段落取得に失敗しました: {error}", paragraph_id))
}

fn read_db_paragraph(row: &Row<'_>) -> rusqlite::Result<DbParagraph> {
    Ok(DbParagraph {
        paragraph_id: row.get(0)?,
        document_id: row.get(1)?,
        paragraph_no: row.get(2)?,
        paragraph_text: row.get(3)?,
    })
}
