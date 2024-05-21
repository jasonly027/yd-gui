use std::path::{Path, PathBuf};

use const_format::formatcp;
use sqlx::{migrate::MigrateError, sqlite::SqliteConnectOptions, Result as sqlxResult, *};

use crate::video::{VideoFormat, VideoInfo};

pub struct Database<DB: sqlx::database::Database> {
    pool: Pool<DB>,
}

impl Database<Sqlite> {
    /// Initialize the database reading from the SQLite database file
    /// supplied by [`Self::get_file_path`].
    ///
    /// If the file does not exist, it will be created.
    ///
    /// See also [`Self::init_with_filename`]
    pub async fn init() -> sqlxResult<Self> {
        Self::init_with_filename(Self::get_file_path()?).await
    }

    /// Initialize the database reading from the SQLite database file
    /// at the `path`. [`Self::init`] is most likely what you want to use
    /// for default behavior.
    ///
    /// If the file does not exist, it will be created.
    pub async fn init_with_filename(path: impl AsRef<Path>) -> sqlxResult<Self> {
        let opts = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(opts).await?;

        let db = Database { pool };
        db.apply_migrations().await?;

        Ok(db)
    }

    /// Get the default path of where the `.db` file should be located.
    /// The path is intended to be in the same directory as the executable.
    ///
    /// # Errors
    /// May fail with an [`std::io::Error`] when getting the path to the
    /// running executable because it's used to derive the path to the `.db` file.
    pub fn get_file_path() -> std::result::Result<PathBuf, std::io::Error> {
        const FILE_NAME: &str = "history.db";

        let mut path = std::env::current_exe()?;
        path.pop();
        path.push(FILE_NAME);

        Ok(path)
    }

    pub async fn apply_migrations(&self) -> sqlxResult<(), MigrateError> {
        migrate!().run(&self.pool).await
    }

    pub async fn close(self) {
        self.pool.close().await;
    }

    async fn get_transaction(&self) -> sqlxResult<Transaction<Sqlite>> {
        self.pool.begin().await
    }
}

const ID: &str = "id";

const VIDEO_INFO: &str = "VIDEO_INFO";
const VIDEO_ID: &str = "video_id";
const TITLE: &str = "title";
const AUTHOR: &str = "author";
const DURATION_SECONDS: &str = "duration_seconds";
const THUMBNAIL: &str = "thumbnail";
const AUDIO_AVAILABLE: &str = "audio_available";

const VIDEO_FORMAT: &str = "video_format";
const CONTAINER: &str = "container";
const WIDTH: &str = "width";
const HEIGHT: &str = "height";
const FPS: &str = "fps";
const VIDEO_INFO_ID: &str = "video_info_id";

impl Database<Sqlite> {
    pub async fn insert_video_info(&self, video_info: &VideoInfo) -> sqlxResult<i32> {
        let mut transaction = self.get_transaction().await?;

        // Insertion into video_info table
        const QUERY_VIDEO_INFO: &str = formatcp!(
            "INSERT INTO {VIDEO_INFO}
                ({VIDEO_ID}, {TITLE}, {AUTHOR},
                    {DURATION_SECONDS}, {THUMBNAIL}, {AUDIO_AVAILABLE})
             VALUES
                ($1, $2, $3,
                    $4, $5, $6)
             RETURNING
                {ID}
            "
        );
        let id: i32 = query_scalar(QUERY_VIDEO_INFO)
            .bind(&video_info.video_id)
            .bind(&video_info.title)
            .bind(&video_info.author)
            .bind(&video_info.duration_seconds)
            .bind(&video_info.thumbnail)
            .bind(&video_info.audio_available)
            .fetch_one(&mut *transaction)
            .await?;

        // Insertion(s) into video_format table
        const QUERY_VIDEO_FORMAT: &str = formatcp!(
            "INSERT INTO {VIDEO_FORMAT}
                ({CONTAINER}, {WIDTH}, {HEIGHT}, {FPS}, {VIDEO_INFO_ID})
             VALUES
                ($1, $2, $3, $4, $5)
            "
        );
        for video_format in &video_info.video_formats {
            query(&QUERY_VIDEO_FORMAT)
                .bind(&video_format.container)
                .bind(&video_format.width)
                .bind(&video_format.height)
                .bind(&video_format.fps)
                .bind(&id)
                .execute(&mut *transaction)
                .await?;
        }

        transaction.commit().await?;

        Ok(id)
    }

    pub async fn bulk_insert_video_info(&self, video_info: &Vec<VideoInfo>) -> sqlxResult<Vec<i32>> {
        let mut transaction = self.get_transaction().await?;

        todo!();

        transaction.commit().await?;

    }

    pub async fn delete_video_info(&self, id: &i32) -> sqlxResult<u64> {
        const QUERY: &str = formatcp!(
            "DELETE FROM {VIDEO_INFO} WHERE {ID} = $1"
        );
        let result = query(QUERY)
            .bind(&id)
            .execute(&self.pool)
            .await?;
        
        Ok(result.rows_affected())
    }

    pub async fn delete_all(&self) ->sqlxResult<u64> {
        const QUERY: &str = formatcp!(
            "DELETE FROM {VIDEO_INFO}"
        );
        let result = query(QUERY)
            .execute(&self.pool)
            .await?;

        Ok(result.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use crate::video::{VideoFormat, VideoInfo};

    use super::Database;
    use anyhow::{Ok, Result};
    use sqlx::{migrate, SqlitePool};

    #[sqlx::test]
    async fn init_database() -> Result<()> {
        let mut path = std::env::current_exe().unwrap();
        path.set_extension("db");

        let db = Database::init_with_filename(&path).await?;

        assert!(path.is_file());

        // Clean up
        db.close().await;
        std::fs::remove_file(&path).unwrap();

        Ok(())
    }

    #[sqlx::test]
    async fn run_migrations(pool: SqlitePool) -> Result<()> {
        Ok(migrate!().run(&pool).await?)
    }

    fn get_example_video_info() -> VideoInfo {
        VideoInfo {
            video_id: "lY2yjAdbvdQ".to_string(),
            title: "Shawn Mendes - Treat You Better".to_string(),
            author: "Shawn Mendes".to_string(),
            duration_seconds: "256".to_string(),
            thumbnail: None,
            video_formats: vec![
                VideoFormat {
                    container: "webm".to_string(),
                    width: "360".to_string(),
                    height: "480".to_string(),
                    fps: "60".to_string(),
                },
                VideoFormat {
                    container: "mp4".to_string(),
                    width: "1280".to_string(),
                    height: "720".to_string(),
                    fps: "120".to_string(),
                },
            ],
            audio_available: true,
        }
    }

    #[sqlx::test]
    async fn insert_one(pool: SqlitePool) {
        let db = Database { pool };

        let video_info = get_example_video_info();
        let id = db.insert_video_info(&video_info).await.unwrap();

        assert_eq!(id, 1);
    }

    #[sqlx::test]
    async fn insert_two_delete_one(pool: SqlitePool) {
        let db = Database { pool };

        let video_info = get_example_video_info();

        let _ = db.insert_video_info(&video_info).await.unwrap();
        let id = db.insert_video_info(&video_info).await.unwrap();

        let deletions = db.delete_video_info(&id).await.unwrap();

        assert_eq!(deletions, 1);
    }

    #[sqlx::test]
    async fn delete_one_on_empty_db(pool: SqlitePool) {
        let db = Database { pool };

        let deletions = db.delete_video_info(&1).await.unwrap();

        assert_eq!(deletions, 0);
    }

    #[sqlx::test]
    async fn insert_many_delete_nonexisting(pool: SqlitePool) {
        let db = Database { pool };

        let video_info = get_example_video_info();

        for _ in 0..4 {
            db.insert_video_info(&video_info).await.unwrap();
        }

        let deletions = db.delete_video_info(&5).await.unwrap();

        assert_eq!(deletions, 0);
    }
}
