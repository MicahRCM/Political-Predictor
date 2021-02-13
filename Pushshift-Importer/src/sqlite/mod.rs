use std::path::Path;

use anyhow::Result;
// use rusqlite::{params, Connection};
use postgres::{Client, NoTls};

use crate::comment::Comment;

const SETUP: &str = include_str!("comment.sql");

pub struct Sqlite {
    connection: Client,
}

impl Sqlite {
    pub fn new(filename: &Path) -> Result<Self> {
        let connection = Client::connect("postgres://postgres:password@localhost/newredditcomments", NoTls)
            .unwrap();

        // let connection = Connection::open(filename).unwrap();
        // connection.execute_batch(SETUP)?;
        Ok(Sqlite { connection })
    }

    pub fn insert_comment(&mut self, comment: &Comment) -> Result<usize> {
        self.connection.execute(
            "INSERT INTO comment (reddit_id, author, subreddit, body, score, created_utc, retrieved_on, parent_id, parent_is_post) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)", &[
            &comment.id.as_str(),
            &comment.author.as_str(),
            &comment.subreddit.as_str(),
            &comment.body.as_str(),
            &comment.score,
            &comment.created_utc,
            &comment.retrieved_on,
            &comment.parent_id.as_str(),
            &comment.parent_is_post
        ]).unwrap();

        Ok(0)
    }
}
