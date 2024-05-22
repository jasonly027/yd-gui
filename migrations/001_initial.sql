-- Create the video_info table
CREATE TABLE IF NOT EXISTS video_info (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id TEXT NOT NULL,
    title TEXT NOT NULL,
    author TEXT NOT NULL,
    duration_seconds TEXT NOT NULL,
    thumbnail TEXT,
    audio_available BOOLEAN NOT NULL
);

-- Create the video_format table
CREATE TABLE IF NOT EXISTS video_format (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    container TEXT NOT NULL,
    width TEXT NOT NULL,
    height TEXT NOT NULL,
    fps TEXT NOT NULL,
    video_info_id INTEGER NOT NULL,
    FOREIGN KEY (video_info_id) REFERENCES video_info (id) ON DELETE CASCADE
);