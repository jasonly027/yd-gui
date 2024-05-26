use std::path::{Path, PathBuf};

use const_format::formatcp;
use sqlx::{
    migrate::MigrateError,
    sqlite::{SqliteConnectOptions, SqliteRow},
    Result as sqlxResult, *,
};

use crate::video::{ManagedVideo, VideoInfo};

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
const QUERY_VIDEO_FORMAT: &str = formatcp!(
    "INSERT INTO {VIDEO_FORMAT}
        ({CONTAINER}, {WIDTH}, {HEIGHT}, {FPS}, {VIDEO_INFO_ID})
     VALUES
        ($1, $2, $3, $4, $5)
    "
);

const QUERY_FETCH_ONE_VIDEO_INFO: &str = formatcp!(
    "SELECT {VIDEO_ID}, {TITLE}, {AUTHOR},
        {DURATION_SECONDS}, {THUMBNAIL}, {AUDIO_AVAILABLE}
     FROM {VIDEO_INFO}
     WHERE {ID} = $1
    "
);

const QUERY_FETCH_ONE_VIDEO_FORMATS: &str = formatcp!(
    "SELECT {CONTAINER}, {WIDTH}, {HEIGHT}, {FPS}, {VIDEO_INFO_ID}
     FROM {VIDEO_FORMAT}
     WHERE {VIDEO_INFO_ID} = $1
    "
);

const QUERY_FETCH_CHUNK_VIDEO_INFO: &str = formatcp!(
    "SELECT {ID}, {VIDEO_ID}, {TITLE}, {AUTHOR},
        {DURATION_SECONDS}, {THUMBNAIL}, {AUDIO_AVAILABLE}
     FROM {VIDEO_INFO}
     WHERE {ID} >= $1
     ORDER BY {ID}
     LIMIT $2
    "
);

pub struct IdAndInfo(i32, VideoInfo);
impl FromRow<'_, SqliteRow> for IdAndInfo {
    fn from_row(row: &SqliteRow) -> Result<Self, sqlx::Error> {
        Ok(Self(
            row.try_get("id")?,
            VideoInfo {
                video_id: row.try_get("video_id")?,
                title: row.try_get("title")?,
                author: row.try_get("author")?,
                duration_seconds: row.try_get("duration_seconds")?,
                thumbnail: row.try_get("thumbnail")?,
                video_formats: Vec::default(),
                audio_available: row.try_get("audio_available")?,
            },
        ))
    }
}

impl Database<Sqlite> {
    pub async fn fetch_one(&self, id: i32) -> sqlxResult<ManagedVideo> {
        let mut video_info: VideoInfo = query_as(QUERY_FETCH_ONE_VIDEO_INFO)
            .bind(id)
            .fetch_one(&self.pool)
            .await?;

        video_info.video_formats = query_as(QUERY_FETCH_ONE_VIDEO_FORMATS)
            .bind(id)
            .fetch_all(&self.pool)
            .await?;

        Ok(ManagedVideo::new(id, video_info))
    }

    pub async fn fetch_chunk_of(
        &self,
        starting_id: i32,
        num_entries: u32,
    ) -> sqlxResult<Vec<ManagedVideo>> {
        let id_and_infos: Vec<IdAndInfo> = query_as(QUERY_FETCH_CHUNK_VIDEO_INFO)
            .bind(starting_id)
            .bind(num_entries)
            .fetch_all(&self.pool)
            .await?;

        let mut managed_videos = Vec::new();
        for IdAndInfo(id, mut video_info) in id_and_infos {
            video_info.video_formats = query_as(QUERY_FETCH_ONE_VIDEO_FORMATS)
                .bind(id)
                .fetch_all(&self.pool)
                .await?;
            let managed_video = ManagedVideo::new(id, video_info);
            managed_videos.push(managed_video);
        }

        Ok(managed_videos)
    }

    pub async fn fetch_chunk(&self, starting_id: i32) -> sqlxResult<Vec<ManagedVideo>> {
        self.fetch_chunk_of(starting_id, 20).await
    }

    pub async fn insert_video_info(&self, video_info: &VideoInfo) -> sqlxResult<i32> {
        let mut transaction = self.get_transaction().await?;

        // Insertion into video_info table
        let id: i32 = query_scalar(QUERY_VIDEO_INFO)
            .bind(&video_info.video_id)
            .bind(&video_info.title)
            .bind(&video_info.author)
            .bind(&video_info.duration_seconds)
            .bind(&video_info.thumbnail)
            .bind(video_info.audio_available)
            .fetch_one(&mut *transaction)
            .await?;

        // Insertion(s) into video_format table
        for video_format in &video_info.video_formats {
            query(QUERY_VIDEO_FORMAT)
                .bind(&video_format.container)
                .bind(&video_format.width)
                .bind(&video_format.height)
                .bind(&video_format.fps)
                .bind(id)
                .execute(&mut *transaction)
                .await?;
        }

        transaction.commit().await?;

        Ok(id)
    }

    pub async fn insert_bulk_video_info(
        &self,
        video_infos: &Vec<VideoInfo>,
    ) -> sqlxResult<Vec<i32>> {
        let mut transaction = self.get_transaction().await?;

        let mut res = Vec::with_capacity(video_infos.len());
        for video_info in video_infos {
            let id: i32 = query_scalar(QUERY_VIDEO_INFO)
                .bind(&video_info.video_id)
                .bind(&video_info.title)
                .bind(&video_info.author)
                .bind(&video_info.duration_seconds)
                .bind(&video_info.thumbnail)
                .bind(video_info.audio_available)
                .fetch_one(&mut *transaction)
                .await?;
            for video_format in &video_info.video_formats {
                query(QUERY_VIDEO_FORMAT)
                    .bind(&video_format.container)
                    .bind(&video_format.width)
                    .bind(&video_format.height)
                    .bind(&video_format.fps)
                    .bind(id)
                    .execute(&mut *transaction)
                    .await?;
            }
            res.push(id);
        }

        transaction.commit().await?;

        Ok(res)
    }

    pub async fn delete_video_info(&self, id: i32) -> sqlxResult<u64> {
        const QUERY: &str = formatcp!("DELETE FROM {VIDEO_INFO} WHERE {ID} = $1");
        let result = query(QUERY).bind(id).execute(&self.pool).await?;

        Ok(result.rows_affected())
    }

    pub async fn delete_all(&self) -> sqlxResult<u64> {
        const QUERY: &str = formatcp!("DELETE FROM {VIDEO_INFO}");
        let result = query(QUERY).execute(&self.pool).await?;

        Ok(result.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use crate::video::{ManagedVideo, VideoFormat, VideoInfo};

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

    fn get_test_videos() -> Vec<VideoInfo> {
        vec![
            VideoInfo {
                video_id: "id1".to_string(),
                title: "Video 1".to_string(),
                author: "Author 1".to_string(),
                duration_seconds: "1".to_string(),
                thumbnail: None,
                video_formats: vec![
                    VideoFormat {
                        container: "webm".to_string(),
                        width: "640".to_string(),
                        height: "480".to_string(),
                        fps: "30".to_string(),
                    },
                    VideoFormat {
                        container: "mp4".to_string(),
                        width: "1280".to_string(),
                        height: "720".to_string(),
                        fps: "60".to_string(),
                    },
                ],
                audio_available: true,
            },
            VideoInfo {
                video_id: "id2".to_string(),
                title: "Video 2".to_string(),
                author: "Author 2".to_string(),
                duration_seconds: "2".to_string(),
                thumbnail: None,
                video_formats: vec![
                    VideoFormat {
                        container: "webm".to_string(),
                        width: "640".to_string(),
                        height: "480".to_string(),
                        fps: "30".to_string(),
                    },
                    VideoFormat {
                        container: "mp4".to_string(),
                        width: "1280".to_string(),
                        height: "720".to_string(),
                        fps: "60".to_string(),
                    },
                ],
                audio_available: false,
            },
            VideoInfo {
                video_id: "id3".to_string(),
                title: "Video 3".to_string(),
                author: "Author 3".to_string(),
                duration_seconds: "3".to_string(),
                thumbnail: None,
                video_formats: vec![
                    VideoFormat {
                        container: "webm".to_string(),
                        width: "640".to_string(),
                        height: "480".to_string(),
                        fps: "30".to_string(),
                    },
                    VideoFormat {
                        container: "mp4".to_string(),
                        width: "1280".to_string(),
                        height: "720".to_string(),
                        fps: "60".to_string(),
                    },
                ],
                audio_available: true,
            },
        ]
    }

    #[sqlx::test]
    async fn fetch_one(pool: SqlitePool) {
        let db = Database { pool };

        let test_video = &get_test_videos()[0];

        let id = db.insert_video_info(test_video).await.unwrap();

        let db_video = db.fetch_one(id).await.unwrap();

        assert_eq!(db_video.get_info(), test_video);
    }

    #[sqlx::test]
    async fn fetch_chunk_of(pool: SqlitePool) {
        let db = Database { pool };

        let test_videos = get_test_videos();

        db.insert_bulk_video_info(&test_videos).await.unwrap();

        let db_videos: Vec<VideoInfo> = db
            .fetch_chunk_of(1, test_videos.len() as u32)
            .await
            .unwrap()
            .into_iter()
            .map(ManagedVideo::into)
            .collect();

        assert_eq!(db_videos, test_videos);
    }

    #[sqlx::test]
    async fn fetch_chunk(pool: SqlitePool) {
        let db = Database { pool };

        let test_videos = get_test_videos();

        db.insert_bulk_video_info(&test_videos).await.unwrap();

        let db_videos: Vec<VideoInfo> = db
            .fetch_chunk(1)
            .await
            .unwrap()
            .into_iter()
            .map(ManagedVideo::into)
            .collect();

        assert_eq!(db_videos, test_videos);
    }

    #[sqlx::test]
    async fn insert_one(pool: SqlitePool) {
        let db = Database { pool };

        let test_video = &get_test_videos()[0];
        let id = db.insert_video_info(test_video).await.unwrap();

        // Check primary key id
        assert_eq!(id, 1);

        // Check video_info
        let db_video = db.fetch_one(id).await.unwrap();
        assert_eq!(db_video.get_info(), test_video);
    }

    #[sqlx::test]
    async fn insert_two_delete_one(pool: SqlitePool) {
        let db = Database { pool };

        let test_videos = get_test_videos();

        let first_id = db.insert_video_info(&test_videos[0]).await.unwrap();
        let second_id = db.insert_video_info(&test_videos[1]).await.unwrap();

        // Check primary key ids
        assert_eq!(first_id, 1);
        assert_eq!(second_id, 2);

        // Check video_infos
        let first_video = db.fetch_one(first_id).await.unwrap();
        let second_video = db.fetch_one(second_id).await.unwrap();

        assert_eq!(&test_videos[0], first_video.get_info());
        assert_eq!(&test_videos[1], second_video.get_info());

        // Check successful deletion
        let rows_deleted = db.delete_video_info(first_id).await.unwrap();
        assert_eq!(rows_deleted, 1);
    }

    #[sqlx::test]
    async fn bulk_insert(pool: SqlitePool) {
        let db = Database { pool };

        let test_videos = get_test_videos();
        let ids = db.insert_bulk_video_info(&test_videos).await.unwrap();

        // Check primary key ids
        assert_eq!(test_videos.len(), ids.len());
        for (i, id) in ids.iter().enumerate() {
            assert_eq!(*id as usize, i + 1);
        }

        // Fetch videos
        let mut db_videos = Vec::new();
        for id in ids {
            let managed_video = db.fetch_one(id).await.unwrap();
            db_videos.push(managed_video);
        }

        // Check video_infos
        for (i, vid) in db_videos.iter().enumerate() {
            assert_eq!(vid.get_info(), &test_videos[i])
        }
    }

    #[sqlx::test]
    async fn delete_one_on_empty_db(pool: SqlitePool) {
        let db = Database { pool };

        let deletions = db.delete_video_info(1).await.unwrap();

        assert_eq!(
            deletions, 0,
            "There should be no deletion because there is nothing in the database to delete"
        );
    }

    #[sqlx::test]
    async fn insert_many_delete_nonexisting(pool: SqlitePool) {
        let db = Database { pool };

        let test_video = &get_test_videos()[0];

        for _ in 0..4 {
            db.insert_video_info(test_video).await.unwrap();
        }

        let deletions = db.delete_video_info(5).await.unwrap();

        assert_eq!(
            deletions, 0,
            "There should be no deletion because the id targeted is one more than the greatest id"
        );
    }

    #[sqlx::test]
    async fn delete_all(pool: SqlitePool) {
        let db = Database { pool };

        let test_videos = get_test_videos();
        let ids = db.insert_bulk_video_info(&test_videos).await.unwrap();

        let rows_deleted = db.delete_all().await.unwrap();

        assert_eq!(
            ids.len() as u64,
            rows_deleted,
            "The number of deleted rows should be equal to the number of videos inserted"
        );
    }
}
