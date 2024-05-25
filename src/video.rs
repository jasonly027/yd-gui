use std::sync::{atomic::AtomicBool, Arc};

#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct VideoInfo {
    pub video_id: String,
    pub title: String,
    pub author: String,
    pub duration_seconds: String,
    pub thumbnail: Option<String>,
    #[sqlx(skip)]
    pub video_formats: Vec<VideoFormat>,
    pub audio_available: bool,
}

#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct VideoFormat {
    pub container: String,
    pub width: String,
    pub height: String,
    pub fps: String,
}

#[derive(Debug, Clone)]
pub struct ManagedVideo {
    id: i32,
    video_info: VideoInfo,
    content_size: Option<u64>,
    downloading: Arc<AtomicBool>,
}

impl ManagedVideo {
    pub fn new(id: i32, video_info: VideoInfo) -> Self {
        Self {
            id,
            video_info,
            content_size: None,
            downloading: Arc::new(AtomicBool::new(false))
        }
    }

    pub fn get_info(&self) -> &VideoInfo {
        &self.video_info
    }
}
