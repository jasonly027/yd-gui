use std::sync::{atomic::AtomicBool, Arc};

#[derive(Debug, Clone)]
pub struct VideoInfo {
    pub video_id: String,
    pub title: String,
    pub author: String,
    pub duration_seconds: String,
    pub thumbnail: Option<String>,
    pub video_formats: Vec<VideoFormat>,
    pub audio_available: bool,
}

#[derive(Debug, Clone)]
pub struct VideoFormat {
    pub container: String,
    pub width: String,
    pub height: String,
    pub fps: String,
}

#[derive(Debug, Clone)]
struct ManagedVideo {
    id: i64,
    video_info: VideoInfo,
    content_size: Option<u64>,
    downloading: Arc<AtomicBool>,
}
