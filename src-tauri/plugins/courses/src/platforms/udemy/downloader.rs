use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use futures::StreamExt;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;

use omniget_core::core::media_processor::MediaProcessor;

use super::api::{self, UdemyCourse};
use super::auth::UdemySession;

const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0";
const MAX_FILENAME_BYTES: usize = 200;

#[derive(Debug, Clone, serde::Serialize)]
pub struct UdemyCourseDownloadProgress {
    pub course_id: u64,
    pub course_name: String,
    pub percent: f64,
    pub current_chapter: String,
    pub current_lecture: String,
    pub downloaded_bytes: u64,
    pub total_lectures: u32,
    pub completed_lectures: u32,
}

pub struct UdemyDownloader {
    session: Arc<Mutex<Option<UdemySession>>>,
    hls_client: reqwest::Client,
    max_concurrent_segments: u32,
    max_retries: u32,
    download_subtitles: bool,
    caption_locale: String,
    include_auto_subtitles: bool,
}

fn select_caption_track<'a>(
    tracks: &'a [serde_json::Value],
    locale: &str,
    include_auto_subtitles: bool,
) -> Option<&'a serde_json::Value> {
    let wanted = locale.trim().to_ascii_lowercase().replace('_', "-");
    if wanted.is_empty() {
        return None;
    }
    tracks
        .iter()
        .filter(|track| {
            if track.get("_class").and_then(|value| value.as_str()) != Some("caption")
                || track.get("url").and_then(|value| value.as_str()).is_none()
                || (!include_auto_subtitles
                    && track.get("source").and_then(|value| value.as_str()) == Some("auto"))
            {
                return false;
            }
            let language = track
                .get("locale_id")
                .or_else(|| track.get("language"))
                .or_else(|| track.get("srclang"))
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_ascii_lowercase()
                .replace('_', "-");
            language == wanted || language.starts_with(&format!("{}-", wanted))
        })
        .min_by_key(|track| track.get("source").and_then(|value| value.as_str()) == Some("auto"))
}

fn safe_filename(name: &str) -> String {
    let sanitized = sanitize_filename::sanitize(name);
    if sanitized.len() <= MAX_FILENAME_BYTES {
        return sanitized;
    }
    let mut end = MAX_FILENAME_BYTES;
    while !sanitized.is_char_boundary(end) && end > 0 {
        end -= 1;
    }
    sanitized[..end].to_string()
}

impl UdemyDownloader {
    pub fn new(
        session: Arc<Mutex<Option<UdemySession>>>,
        max_concurrent_segments: u32,
        max_retries: u32,
        download_subtitles: bool,
        caption_locale: String,
        include_auto_subtitles: bool,
    ) -> Self {
        let hls_client =
            omniget_core::core::http_client::apply_global_proxy(reqwest::Client::builder())
                .user_agent(USER_AGENT)
                .connect_timeout(Duration::from_secs(30))
                .timeout(Duration::from_secs(300))
                .build()
                .unwrap_or_default();

        Self {
            session,
            hls_client,
            max_concurrent_segments,
            max_retries,
            download_subtitles,
            caption_locale,
            include_auto_subtitles,
        }
    }

    pub async fn download_full_course(
        &self,
        course: &UdemyCourse,
        output_dir: &str,
        curriculum: api::UdemyCurriculum,
        progress_tx: mpsc::Sender<UdemyCourseDownloadProgress>,
        cancel_token: CancellationToken,
    ) -> anyhow::Result<u32> {
        let session = {
            let guard = self.session.lock().await;
            guard.clone().ok_or_else(|| anyhow!("Not authenticated"))?
        };

        let _ = progress_tx
            .send(UdemyCourseDownloadProgress {
                course_id: course.id,
                course_name: course.title.clone(),
                percent: 0.0,
                current_chapter: String::new(),
                current_lecture: String::new(),
                downloaded_bytes: 0,
                total_lectures: curriculum.total_lectures,
                completed_lectures: 0,
            })
            .await;

        let course_dir_name = safe_filename(&course.title);
        let course_dir = PathBuf::from(output_dir).join(&course_dir_name);
        tokio::fs::create_dir_all(&course_dir).await?;

        let total_lectures = curriculum.total_lectures;
        let mut completed_lectures: u32 = 0;
        let mut downloaded_bytes: u64 = 0;
        let mut drm_skipped: u32 = 0;
        let mut failures = Vec::new();

        for (ch_idx, chapter) in curriculum.chapters.iter().enumerate() {
            if cancel_token.is_cancelled() {
                return Err(anyhow!("Download cancelled"));
            }

            let chapter_dir_name = format!("{:02} - {}", ch_idx + 1, safe_filename(&chapter.title));
            let chapter_dir = course_dir.join(&chapter_dir_name);
            tokio::fs::create_dir_all(&chapter_dir).await?;

            for (lec_idx, lecture) in chapter.lectures.iter().enumerate() {
                if cancel_token.is_cancelled() {
                    return Err(anyhow!("Download cancelled"));
                }

                let lecture_num = (lec_idx + 1) as u32;

                let _ = progress_tx
                    .send(UdemyCourseDownloadProgress {
                        course_id: course.id,
                        course_name: course.title.clone(),
                        percent: if total_lectures > 0 {
                            (completed_lectures as f64 / total_lectures as f64) * 100.0
                        } else {
                            0.0
                        },
                        current_chapter: chapter.title.clone(),
                        current_lecture: format!(
                            "{}/{} - {}",
                            lecture_num,
                            chapter.lectures.len(),
                            lecture.title
                        ),
                        downloaded_bytes,
                        total_lectures,
                        completed_lectures,
                    })
                    .await;

                let result = self
                    .download_lecture(&session, lecture, &chapter_dir, &cancel_token, lecture_num)
                    .await;

                match result {
                    Ok((b, drm)) => {
                        downloaded_bytes += b;
                        drm_skipped += drm;
                    }
                    Err(e) => {
                        tracing::error!(
                            "[udemy] failed to download lecture '{}': {}",
                            lecture.title,
                            e
                        );
                        failures.push(format!("{}: {}", lecture.title, e));
                    }
                }

                if lecture.lecture_class == "lecture" {
                    completed_lectures += 1;
                }
            }
        }

        let _ = progress_tx
            .send(UdemyCourseDownloadProgress {
                course_id: course.id,
                course_name: course.title.clone(),
                percent: 100.0,
                current_chapter: String::new(),
                current_lecture: String::new(),
                downloaded_bytes,
                total_lectures,
                completed_lectures,
            })
            .await;

        tracing::info!(
            "[udemy] course '{}' download complete: {} lectures, {} bytes, {} drm-skipped",
            course.title,
            completed_lectures,
            downloaded_bytes,
            drm_skipped
        );

        if !failures.is_empty() {
            return Err(anyhow!(
                "{} lecture(s) could not be processed. First error: {}",
                failures.len(),
                failures[0]
            ));
        }
        Ok(drm_skipped)
    }

    async fn download_lecture(
        &self,
        session: &UdemySession,
        lecture: &api::UdemyLecture,
        chapter_dir: &Path,
        cancel_token: &CancellationToken,
        lecture_num: u32,
    ) -> anyhow::Result<(u64, u32)> {
        if lecture.lecture_class == "quiz" || lecture.lecture_class == "practice" {
            tracing::info!(
                "[udemy] skipping {}: {}",
                lecture.lecture_class,
                lecture.title
            );
            return Ok((0, 0));
        }

        let asset = match &lecture.asset {
            Some(a) => a,
            None => return Ok((0, 0)),
        };

        let asset_type = asset
            .get("asset_type")
            .or_else(|| asset.get("assetType"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_lowercase();

        let mut total_bytes: u64 = 0;
        let mut drm_skipped: u32 = 0;

        match asset_type.as_str() {
            "video" => {
                let (bytes, drm) = self
                    .download_video_asset(
                        session,
                        asset,
                        &lecture.title,
                        chapter_dir,
                        cancel_token,
                        lecture_num,
                    )
                    .await?;
                total_bytes += bytes;
                drm_skipped += drm;
            }
            "article" => {
                let body = asset.get("body").and_then(|v| v.as_str()).unwrap_or("");
                if !body.is_empty() {
                    let file_name = format!(
                        "{:02} - {}.html",
                        lecture_num,
                        safe_filename(&lecture.title)
                    );
                    let file_path = chapter_dir.join(&file_name);
                    if !file_exists_with_content(&file_path).await {
                        tokio::fs::write(&file_path, body.as_bytes()).await?;
                        tracing::info!("[udemy] saved article: {}", file_name);
                    }
                }
            }
            "file" | "e-book" | "presentation" | "audio" => {
                total_bytes += self
                    .download_downloadable_asset(
                        session,
                        asset,
                        &lecture.title,
                        chapter_dir,
                        lecture_num,
                    )
                    .await?;
            }
            _ => {
                if !asset_type.is_empty() {
                    tracing::warn!(
                        "[udemy] unknown asset type '{}' for '{}'",
                        asset_type,
                        lecture.title
                    );
                }
            }
        }

        if asset_type == "video" {
            if drm_skipped > 0 {
                return Ok((total_bytes, drm_skipped));
            }
            let video_path = chapter_dir.join(format!(
                "{:02} - {}.mp4",
                lecture_num,
                safe_filename(&lecture.title)
            ));
            if !file_exists_with_content(&video_path).await {
                return Err(anyhow!("No video file available for '{}'", lecture.title));
            }
        }

        if self.download_subtitles {
            if let Some(tracks) = asset.get("captions").and_then(|v| v.as_array()) {
                if let Some(track) =
                    select_caption_track(tracks, &self.caption_locale, self.include_auto_subtitles)
                {
                    let url = track
                        .get("url")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default();
                    let lang = track
                        .get("locale_id")
                        .or_else(|| track.get("language"))
                        .or_else(|| track.get("srclang"))
                        .and_then(|v| v.as_str())
                        .unwrap_or(&self.caption_locale);
                    let caption_name = format!(
                        "{:02} - {}_{}.vtt",
                        lecture_num,
                        safe_filename(&lecture.title),
                        safe_filename(lang)
                    );
                    let caption_path = chapter_dir.join(&caption_name);
                    let video_path = chapter_dir.join(format!(
                        "{:02} - {}.mp4",
                        lecture_num,
                        safe_filename(&lecture.title)
                    ));

                    if asset_type == "video" && file_exists_with_content(&video_path).await {
                        omniget_core::core::dependencies::ensure_ffmpeg().await?;
                    }

                    if asset_type == "video"
                        && file_exists_with_content(&video_path).await
                        && has_burned_captions(&video_path, &self.caption_locale).await?
                    {
                        tracing::info!("[udemy] subtitles already burned into '{}'", lecture.title);
                        continue_caption_assets(&caption_path).await?;
                    } else {
                        if file_exists_with_content(&caption_path).await {
                            match restyle_existing_caption(&caption_path).await {
                                Ok(true) => {
                                    tracing::info!(
                                        "[udemy] applied subtitle style: {}",
                                        caption_name
                                    );
                                }
                                Ok(false) => {}
                                Err(e) => {
                                    tracing::warn!(
                                        "[udemy] failed to style existing caption '{}': {}",
                                        caption_name,
                                        e
                                    );
                                    return Err(anyhow!(
                                        "Failed to prepare subtitles '{}': {}",
                                        caption_name,
                                        e
                                    ));
                                }
                            }
                        } else {
                            match download_caption_styled(&session.client, url, &caption_path).await
                            {
                                Ok(b) => {
                                    total_bytes += b;
                                    tracing::info!("[udemy] saved caption: {}", caption_name);
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        "[udemy] failed to download caption '{}': {}",
                                        caption_name,
                                        e
                                    );
                                    return Err(anyhow!(
                                        "Failed to download subtitles '{}': {}",
                                        caption_name,
                                        e
                                    ));
                                }
                            }
                        }
                        if asset_type == "video"
                            && file_exists_with_content(&video_path).await
                            && file_exists_with_content(&caption_path).await
                        {
                            burn_captions_into_video(
                                &video_path,
                                &caption_path,
                                &self.caption_locale,
                                cancel_token,
                            )
                            .await?;
                        }
                    }
                } else {
                    tracing::info!(
                        "[udemy] no '{}' subtitles available for '{}'",
                        self.caption_locale,
                        lecture.title
                    );
                    if asset_type == "video" && drm_skipped == 0 {
                        return Err(anyhow!(
                            "No '{}' subtitles available for '{}'",
                            self.caption_locale,
                            lecture.title
                        ));
                    }
                }
            } else if asset_type == "video" && drm_skipped == 0 {
                return Err(anyhow!(
                    "No subtitle tracks available for '{}'",
                    lecture.title
                ));
            }
        }

        let supp_assets = asset.get("supplementary_assets").and_then(|v| v.as_array());
        if let Some(assets) = supp_assets {
            for supp in assets {
                total_bytes += self
                    .download_supplementary_asset(session, supp, chapter_dir, lecture_num)
                    .await
                    .unwrap_or(0);
            }
        }

        Ok((total_bytes, drm_skipped))
    }

    async fn download_video_asset(
        &self,
        session: &UdemySession,
        asset: &serde_json::Value,
        title: &str,
        chapter_dir: &Path,
        cancel_token: &CancellationToken,
        lecture_num: u32,
    ) -> anyhow::Result<(u64, u32)> {
        let file_name = format!("{:02} - {}.mp4", lecture_num, safe_filename(title));
        let file_path = chapter_dir.join(&file_name);

        if file_exists_with_content(&file_path).await {
            tracing::info!("[udemy] skipping existing video: {}", file_name);
            return Ok((0, 0));
        }

        let stream_urls = asset.get("stream_urls");
        if let Some(streams) = stream_urls {
            if let Some(videos) = streams.get("Video").and_then(|v| v.as_array()) {
                match self
                    .download_from_stream_urls(session, videos, &file_path, title, cancel_token)
                    .await
                {
                    Ok(bytes) => return Ok((bytes, 0)),
                    Err(e) if e.to_string().contains("SAMPLE-AES") => {
                        tracing::warn!(
                            "[udemy] DRM-protected (SAMPLE-AES) via stream_urls: '{}'",
                            title
                        );
                        return Ok((0, 1));
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        if let Some(sources) = asset.get("media_sources").and_then(|v| v.as_array()) {
            let result = self
                .download_from_media_sources(session, sources, &file_path, title, cancel_token)
                .await;

            match result {
                Ok(bytes) if bytes > 0 => return Ok((bytes, 0)),
                Ok(_) => {}
                Err(e) => {
                    let is_drm = asset
                        .get("course_is_drmed")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    if is_drm || e.to_string().contains("SAMPLE-AES") {
                        tracing::warn!("[udemy] DRM-protected video skipped: '{}'", title);
                        return Ok((0, 1));
                    }
                    tracing::warn!(
                        "[udemy] media_sources download failed for '{}': {}",
                        title,
                        e
                    );
                    return Err(e);
                }
            }

            let is_drm = asset
                .get("course_is_drmed")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            if is_drm {
                tracing::warn!(
                    "[udemy] DRM-protected video skipped: '{}' (no downloadable sources available)",
                    title
                );
                return Ok((0, 1));
            }

            tracing::warn!(
                "[udemy] no usable video sources in media_sources for '{}' (types: {})",
                title,
                sources
                    .iter()
                    .filter_map(|s| s.get("type").and_then(|t| t.as_str()))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            return Ok((0, 0));
        }

        tracing::warn!("[udemy] no video sources found for '{}'", title);
        Ok((0, 0))
    }

    async fn download_from_stream_urls(
        &self,
        session: &UdemySession,
        videos: &[serde_json::Value],
        file_path: &Path,
        title: &str,
        cancel_token: &CancellationToken,
    ) -> anyhow::Result<u64> {
        let mut sources: Vec<(&str, u32)> = Vec::new();
        for v in videos {
            let label = v.get("label").and_then(|l| l.as_str()).unwrap_or("0");
            let url = match v.get("file").and_then(|f| f.as_str()) {
                Some(u) => u,
                None => continue,
            };
            if label.to_lowercase() == "audio" {
                continue;
            }
            let height: u32 = label.parse().unwrap_or(0);
            sources.push((url, height));
        }

        if sources.is_empty() {
            tracing::warn!("[udemy] no valid video sources for '{}'", title);
            return Ok(0);
        }

        sources.sort_by(|a, b| b.1.cmp(&a.1));
        let (best_url, best_height) = sources[0];

        tracing::info!("[udemy] downloading '{}' at {}p", title, best_height);

        let url_str = best_url.to_string();
        let is_hls = url_str.contains(".m3u8");

        if is_hls {
            self.download_hls_and_remux(&url_str, file_path, title, cancel_token)
                .await
        } else {
            let bytes = download_file_simple(&session.client, &url_str, file_path).await?;
            tracing::info!(
                "[udemy] direct download complete: {} ({}p, {} bytes)",
                title,
                best_height,
                bytes
            );
            Ok(bytes)
        }
    }

    async fn download_from_media_sources(
        &self,
        session: &UdemySession,
        sources: &[serde_json::Value],
        file_path: &Path,
        title: &str,
        cancel_token: &CancellationToken,
    ) -> anyhow::Result<u64> {
        let mut mp4_sources: Vec<(&str, u32)> = Vec::new();
        let mut hls_source: Option<&str> = None;

        for source in sources {
            let src = match source.get("src").and_then(|v| v.as_str()) {
                Some(s) => s,
                None => continue,
            };
            let media_type = source.get("type").and_then(|v| v.as_str()).unwrap_or("");

            match media_type {
                "video/mp4" => {
                    let label = source.get("label").and_then(|v| v.as_str()).unwrap_or("0");
                    let height: u32 = label.parse().unwrap_or(0);
                    mp4_sources.push((src, height));
                }
                "application/x-mpegURL" => {
                    if hls_source.is_none() {
                        hls_source = Some(src);
                    }
                }
                _ => {}
            }
        }

        if !mp4_sources.is_empty() {
            mp4_sources.sort_by(|a, b| b.1.cmp(&a.1));
            let (best_url, best_height) = mp4_sources[0];

            tracing::info!(
                "[udemy] downloading '{}' at {}p (via media_sources mp4)",
                title,
                best_height
            );

            let bytes = download_file_simple(&session.client, best_url, file_path).await?;
            tracing::info!(
                "[udemy] direct download complete: {} ({}p, {} bytes)",
                title,
                best_height,
                bytes
            );
            return Ok(bytes);
        }

        if let Some(hls_url) = hls_source {
            tracing::info!("[udemy] downloading '{}' via HLS (media_sources)", title);
            return self
                .download_hls_and_remux(hls_url, file_path, title, cancel_token)
                .await;
        }

        Ok(0)
    }

    async fn download_hls_and_remux(
        &self,
        hls_url: &str,
        file_path: &Path,
        title: &str,
        cancel_token: &CancellationToken,
    ) -> anyhow::Result<u64> {
        let temp_ts_path = file_path.with_extension("ts");
        let output_str_ts = temp_ts_path.to_string_lossy().to_string();
        let output_str_mp4 = file_path.to_string_lossy().to_string();

        let result = MediaProcessor::download_hls(
            hls_url,
            &output_str_ts,
            "https://www.udemy.com/",
            None,
            cancel_token.clone(),
            self.max_concurrent_segments,
            self.max_retries,
            Some(self.hls_client.clone()),
        )
        .await;

        match result {
            Ok(r) => {
                tracing::info!(
                    "[udemy] HLS download complete ({} bytes), remuxing...",
                    r.file_size
                );
                if let Err(e) = MediaProcessor::remux(&output_str_ts, &output_str_mp4).await {
                    tracing::error!("[udemy] Remuxing failed: {}", e);
                    let _ = tokio::fs::remove_file(&temp_ts_path).await;
                    return Err(e);
                }
                let _ = tokio::fs::remove_file(&temp_ts_path).await;
                let final_size = tokio::fs::metadata(file_path).await?.len();
                tracing::info!("[udemy] Remux complete: {}", title);
                Ok(final_size)
            }
            Err(e) => {
                let _ = tokio::fs::remove_file(&temp_ts_path).await;
                Err(e)
            }
        }
    }

    async fn download_downloadable_asset(
        &self,
        session: &UdemySession,
        asset: &serde_json::Value,
        title: &str,
        chapter_dir: &Path,
        lecture_num: u32,
    ) -> anyhow::Result<u64> {
        let filename = asset.get("filename").and_then(|v| v.as_str()).unwrap_or("");

        let download_url = asset.get("download_urls").and_then(|d| {
            if let Some(obj) = d.as_object() {
                for (_key, val) in obj {
                    if let Some(arr) = val.as_array() {
                        if let Some(first) = arr.first() {
                            if let Some(url) = first.get("file").and_then(|f| f.as_str()) {
                                return Some(url.to_string());
                            }
                        }
                    }
                }
            }
            None
        });

        let url = match download_url {
            Some(u) => u,
            None => {
                tracing::warn!("[udemy] no download URL for asset '{}'", title);
                return Ok(0);
            }
        };

        let safe_name = if filename.is_empty() {
            format!("{:02} - {}.bin", lecture_num, safe_filename(title))
        } else {
            format!("{:02} - {}", lecture_num, safe_filename(filename))
        };

        let file_path = chapter_dir.join(&safe_name);

        if file_exists_with_content(&file_path).await {
            tracing::info!("[udemy] skipping existing file: {}", safe_name);
            return Ok(0);
        }

        match download_file_simple(&session.client, &url, &file_path).await {
            Ok(b) => {
                tracing::info!("[udemy] downloaded asset: {}", safe_name);
                Ok(b)
            }
            Err(e) => {
                tracing::error!("[udemy] failed to download '{}': {}", safe_name, e);
                Ok(0)
            }
        }
    }

    async fn download_supplementary_asset(
        &self,
        session: &UdemySession,
        supp: &serde_json::Value,
        chapter_dir: &Path,
        lecture_num: u32,
    ) -> anyhow::Result<u64> {
        let asset_type = supp
            .get("asset_type")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_lowercase();
        let filename = supp.get("filename").and_then(|v| v.as_str()).unwrap_or("");
        let title = supp.get("title").and_then(|v| v.as_str()).unwrap_or("");

        match asset_type.as_str() {
            "file" | "sourcecode" => {
                let download_url = supp.get("download_urls").and_then(|d| {
                    if let Some(obj) = d.as_object() {
                        for (_key, val) in obj {
                            if let Some(arr) = val.as_array() {
                                if let Some(first) = arr.first() {
                                    return first
                                        .get("file")
                                        .and_then(|f| f.as_str())
                                        .map(|s| s.to_string());
                                }
                            }
                        }
                    }
                    None
                });

                let url = match download_url {
                    Some(u) => u,
                    None => return Ok(0),
                };

                let base = if filename.is_empty() {
                    safe_filename(title)
                } else {
                    safe_filename(filename)
                };
                let safe_name = format!("{:02} - {}", lecture_num, base);
                let file_path = chapter_dir.join(&safe_name);

                if file_exists_with_content(&file_path).await {
                    return Ok(0);
                }

                download_file_simple(&session.client, &url, &file_path)
                    .await
                    .ok();
            }
            "externallink" => {
                let external_url = supp
                    .get("external_url")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if !external_url.is_empty() {
                    let base = if filename.is_empty() {
                        safe_filename(title)
                    } else {
                        safe_filename(filename)
                    };
                    let safe_name = format!("{:02} - {}.url", lecture_num, base);
                    let file_path = chapter_dir.join(&safe_name);
                    if !file_exists_with_content(&file_path).await {
                        let content = format!("[InternetShortcut]\nURL={}", external_url);
                        tokio::fs::write(&file_path, content.as_bytes()).await?;
                    }
                }
            }
            _ => {}
        }

        Ok(0)
    }
}

async fn file_exists_with_content(path: &Path) -> bool {
    match tokio::fs::metadata(path).await {
        Ok(m) => m.len() > 0,
        Err(_) => false,
    }
}

async fn continue_caption_assets(caption_path: &Path) -> anyhow::Result<()> {
    if file_exists_with_content(caption_path).await {
        tokio::fs::remove_file(caption_path).await?;
    }
    Ok(())
}

async fn probe_video(path: &Path) -> anyhow::Result<(f64, String)> {
    let output = omniget_core::core::process::command("ffprobe")
        .args([
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=codec_type:format=duration:format_tags=comment",
            "-of",
            "json",
        ])
        .arg(path)
        .output()
        .await?;
    if !output.status.success() {
        return Err(anyhow!(
            "ffprobe failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let json: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    if json["streams"].as_array().is_none_or(|s| s.is_empty()) {
        return Err(anyhow!("Video stream missing"));
    }
    let duration = json["format"]["duration"]
        .as_str()
        .unwrap_or("0")
        .parse::<f64>()?;
    if !duration.is_finite() || duration <= 0.0 {
        return Err(anyhow!("Invalid video duration"));
    }
    Ok((
        duration,
        json["format"]["tags"]["comment"]
            .as_str()
            .unwrap_or("")
            .to_string(),
    ))
}

async fn has_burned_captions(path: &Path, locale: &str) -> anyhow::Result<bool> {
    let (_, comment) = probe_video(path).await?;
    if comment.starts_with("OmniGet burned subtitles: ")
        && comment != format!("OmniGet burned subtitles: {}", locale)
    {
        return Err(anyhow!(
            "This video already has permanently burned subtitles in another language"
        ));
    }
    Ok(comment == format!("OmniGet burned subtitles: {}", locale))
}

async fn burn_captions_into_video(
    video: &Path,
    captions: &Path,
    locale: &str,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let (original_duration, _) = probe_video(video).await?;
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let work = video
        .parent()
        .ok_or_else(|| anyhow!("Missing video directory"))?
        .join(format!(
            ".omniget-subtitles-{}-{}",
            std::process::id(),
            stamp
        ));
    tokio::fs::create_dir(&work).await?;
    let work = tokio::fs::canonicalize(&work).await?;
    let original = tokio::fs::canonicalize(video).await?;
    let temp_video = work.join("result.mp4");
    let backup = work.join("original.mp4");
    let result: anyhow::Result<()> = async {
        let caption_text = tokio::fs::read_to_string(captions).await?;
        if !caption_text.contains("-->") {
            return Err(anyhow!("Subtitle file has no timed cues"));
        }
        let normalized_captions = caption_text.replace("\r\n", "\n").replace('\r', "\n");
        let ffmpeg_captions = normalized_captions
            .split("\n\n")
            .filter(|block| {
                let first_line = block.trim_start().lines().next().unwrap_or("").trim();
                first_line != "STYLE"
            })
            .collect::<Vec<_>>()
            .join("\n\n");
        tokio::fs::write(work.join("captions.vtt"), ffmpeg_captions).await?;
        let mut cmd = omniget_core::core::process::command("ffmpeg");
        cmd.current_dir(&work).args(["-nostdin", "-y", "-i"]).arg(&original)
            .args(["-map", "0:v:0", "-map", "0:a?", "-vf", "subtitles=captions.vtt:force_style='FontName=Arial,FontSize=20,PrimaryColour=&H00FFFFFF,BackColour=&H40000000,BorderStyle=3,Outline=1,Shadow=0'", "-c:v", "libx264", "-pix_fmt", "yuv420p", "-crf", "18", "-preset", "veryfast", "-c:a", "copy", "-map_metadata", "0", "-metadata"])
            .arg(format!("comment=OmniGet burned subtitles: {}", locale))
            .args(["-movflags", "+faststart"]).arg(&temp_video)
            .stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).kill_on_drop(true);
        let mut child = cmd.spawn()?;
        let status = tokio::select! { result = child.wait() => result?, _ = cancel.cancelled() => { child.kill().await.ok(); return Err(anyhow!("Download cancelled")); } };
        if !status.success() { return Err(anyhow!("FFmpeg could not burn subtitles ({})", status)); }
        let (new_duration, tag) = probe_video(&temp_video).await?;
        if (new_duration - original_duration).abs() > 2.0_f64.max(original_duration * 0.02) || tag != format!("OmniGet burned subtitles: {}", locale) {
            return Err(anyhow!("Converted video verification failed"));
        }
        let mut verifier = omniget_core::core::process::command("ffmpeg");
        verifier.args(["-nostdin", "-v", "error", "-xerror", "-i"]).arg(&temp_video)
            .args(["-map", "0:v:0", "-map", "0:a?", "-f", "null", "-"])
            .stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).kill_on_drop(true);
        let mut verifier = verifier.spawn()?;
        let verified = tokio::select! { result = verifier.wait() => result?, _ = cancel.cancelled() => { verifier.kill().await.ok(); return Err(anyhow!("Download cancelled")); } };
        if !verified.success() { return Err(anyhow!("Converted video could not be decoded completely")); }
        if cancel.is_cancelled() { return Err(anyhow!("Download cancelled")); }
        tokio::fs::rename(&original, &backup).await?;
        if let Err(e) = tokio::fs::rename(&temp_video, &original).await {
            if let Err(restore) = tokio::fs::rename(&backup, &original).await { return Err(anyhow!("Could not replace video: {}; original retained at {}: {}", e, backup.display(), restore)); }
            return Err(e.into());
        }
        tokio::fs::remove_file(captions).await?;
        tokio::fs::remove_file(&backup).await?;
        Ok(())
    }.await;
    if !backup.exists() {
        let _ = tokio::fs::remove_dir_all(&work).await;
    }
    result
}

async fn download_file_simple(
    client: &reqwest::Client,
    url: &str,
    output_path: &Path,
) -> anyhow::Result<u64> {
    let part_path = output_path.with_extension(format!(
        "{}.part",
        output_path
            .extension()
            .unwrap_or_default()
            .to_string_lossy()
    ));

    let result = download_file_inner(client, url, output_path, &part_path).await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&part_path).await;
    }
    result
}

const WEBVTT_SUBTITLE_STYLE: &str =
    "STYLE\n::cue { color: #fff; background-color: rgba(0, 0, 0, 0.72); }";

fn style_caption_as_webvtt(content: &str) -> anyhow::Result<String> {
    let normalized = content
        .trim_start_matches('\u{feff}')
        .replace("\r\n", "\n")
        .replace('\r', "\n");
    let trimmed = normalized.trim_start();

    if trimmed.starts_with("WEBVTT") {
        let header_end = normalized
            .find("\n\n")
            .unwrap_or_else(|| normalized.find('\n').unwrap_or(normalized.len()));
        let header = normalized[..header_end].trim_end();
        let body = normalized[header_end..].trim_start();
        let body = if body.starts_with("STYLE\n") {
            body.find("\n\n")
                .map(|style_end| body[style_end + 2..].trim_start())
                .unwrap_or(body)
        } else {
            body
        };
        return Ok(format!(
            "{}\n\n{}\n\n{}\n",
            header, WEBVTT_SUBTITLE_STYLE, body
        ));
    }

    if !normalized.contains("-->") {
        return Err(anyhow!("Downloaded subtitle is neither WebVTT nor SRT"));
    }

    let cues = normalized
        .lines()
        .map(|line| {
            if line.contains("-->") {
                line.replace(',', ".")
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n");

    Ok(format!("WEBVTT\n\n{}\n\n{}\n", WEBVTT_SUBTITLE_STYLE, cues))
}

async fn restyle_existing_caption(path: &Path) -> anyhow::Result<bool> {
    let content = tokio::fs::read_to_string(path).await?;
    let styled = style_caption_as_webvtt(&content)?;
    if styled == content {
        return Ok(false);
    }
    tokio::fs::write(path, styled.as_bytes()).await?;
    Ok(true)
}

async fn download_caption_styled(
    client: &reqwest::Client,
    url: &str,
    output_path: &Path,
) -> anyhow::Result<u64> {
    let response = client
        .get(url)
        .timeout(Duration::from_secs(600))
        .send()
        .await
        .map_err(|e| anyhow!("Subtitle request failed: {}", e))?;

    let status = response.status();
    if !status.is_success() {
        return Err(anyhow!("Subtitle download returned status {}", status));
    }

    let bytes = response
        .bytes()
        .await
        .map_err(|e| anyhow!("Failed to read subtitle response: {}", e))?;
    let content = String::from_utf8_lossy(&bytes);
    let styled = style_caption_as_webvtt(&content)?;
    let part_path = output_path.with_extension("vtt.part");

    if let Err(error) = tokio::fs::write(&part_path, styled.as_bytes()).await {
        let _ = tokio::fs::remove_file(&part_path).await;
        return Err(error.into());
    }
    if let Err(error) = tokio::fs::rename(&part_path, output_path).await {
        let _ = tokio::fs::remove_file(&part_path).await;
        return Err(error.into());
    }

    Ok(bytes.len() as u64)
}

async fn download_file_inner(
    client: &reqwest::Client,
    url: &str,
    output_path: &Path,
    part_path: &Path,
) -> anyhow::Result<u64> {
    let resp = client
        .get(url)
        .timeout(Duration::from_secs(600))
        .send()
        .await
        .map_err(|e| anyhow!("Download request failed: {}", e))?;

    let status = resp.status();
    if !status.is_success() {
        if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
            return Err(anyhow!("Auth error ({}), session may have expired", status));
        }
        return Err(anyhow!("Download returned status {}", status));
    }

    let mut total: u64 = 0;
    let mut stream = resp.bytes_stream();
    let mut file = tokio::fs::File::create(part_path).await?;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| anyhow!("Stream error: {}", e))?;
        tokio::io::AsyncWriteExt::write_all(&mut file, &chunk).await?;
        total += chunk.len() as u64;
    }

    tokio::io::AsyncWriteExt::flush(&mut file).await?;
    drop(file);

    tokio::fs::rename(part_path, output_path).await?;

    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::{burn_captions_into_video, has_burned_captions, select_caption_track};
    use serde_json::json;
    use tokio_util::sync::CancellationToken;

    async fn video_fixture() -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "omniget-burn-test-{}-{}",
            std::process::id(),
            stamp
        ));
        tokio::fs::create_dir(&dir).await.unwrap();
        let video = dir.join("video.mp4");
        let caption = dir.join("caption.vtt");
        let status = omniget_core::core::process::command("ffmpeg")
            .args([
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                "color=white:s=320x240:d=2",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=1000:duration=2",
                "-c:v",
                "libx264",
                "-pix_fmt",
                "yuv420p",
                "-c:a",
                "aac",
                "-shortest",
                "-y",
            ])
            .arg(&video)
            .status()
            .await
            .unwrap();
        assert!(status.success());
        tokio::fs::write(
            &caption,
            "WEBVTT\n\nSTYLE\n::cue { color: #fff; background-color: rgba(0, 0, 0, 0.72); }\n\n00:00:00.000 --> 00:00:02.000\nHola mundo\n",
        )
        .await
        .unwrap();
        (dir, video, caption)
    }

    #[tokio::test]
    async fn burn_subtitles_produces_one_verified_video() {
        let (dir, video, caption) = video_fixture().await;
        let audio_before = audio_hash(&video).await;
        burn_captions_into_video(&video, &caption, "es", &CancellationToken::new())
            .await
            .unwrap();
        assert!(video.exists());
        assert!(!caption.exists());
        assert!(has_burned_captions(&video, "es").await.unwrap());
        assert_eq!(
            audio_hash(&video).await,
            audio_before,
            "audio stream must be copied without recoding"
        );
        let frame = omniget_core::core::process::command("ffmpeg")
            .args(["-v", "error", "-ss", "1", "-i"])
            .arg(&video)
            .args(["-frames:v", "1", "-f", "rawvideo", "-pix_fmt", "gray", "-"])
            .output()
            .await
            .unwrap();
        assert!(frame.status.success());
        assert_eq!(frame.stdout.len(), 320 * 240);
        assert!(
            frame.stdout[320 * 160..]
                .iter()
                .filter(|pixel| **pixel < 64)
                .count()
                > 20,
            "subtitle background should appear on the white video"
        );
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
        tokio::fs::remove_dir_all(dir).await.unwrap();
    }

    async fn audio_hash(path: &std::path::Path) -> Vec<u8> {
        let output = omniget_core::core::process::command("ffmpeg")
            .args(["-v", "error", "-i"])
            .arg(path)
            .args(["-map", "0:a:0", "-c:a", "copy", "-f", "md5", "-"])
            .output()
            .await
            .unwrap();
        assert!(output.status.success());
        output.stdout
    }

    #[tokio::test]
    async fn invalid_subtitles_leave_original_video_and_caption_intact() {
        let (dir, video, caption) = video_fixture().await;
        tokio::fs::write(&caption, "invalid subtitle")
            .await
            .unwrap();
        let original = tokio::fs::read(&video).await.unwrap();
        assert!(
            burn_captions_into_video(&video, &caption, "es", &CancellationToken::new())
                .await
                .is_err()
        );
        assert_eq!(tokio::fs::read(&video).await.unwrap(), original);
        assert_eq!(
            tokio::fs::read_to_string(&caption).await.unwrap(),
            "invalid subtitle"
        );
        tokio::fs::remove_dir_all(dir).await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_subtitles_leave_original_video_and_caption_intact() {
        let (dir, video, caption) = video_fixture().await;
        let original = tokio::fs::read(&video).await.unwrap();
        let token = CancellationToken::new();
        token.cancel();
        assert!(burn_captions_into_video(&video, &caption, "es", &token)
            .await
            .is_err());
        assert_eq!(tokio::fs::read(&video).await.unwrap(), original);
        assert!(caption.exists());
        tokio::fs::remove_dir_all(dir).await.unwrap();
    }

    #[test]
    fn selects_requested_language_instead_of_last_track() {
        let tracks = vec![
            json!({"_class": "caption", "locale_id": "en_US", "url": "en.vtt", "source": "auto"}),
            json!({"_class": "caption", "locale_id": "es_ES", "url": "es.vtt", "source": "auto"}),
            json!({"_class": "caption", "locale_id": "de_DE", "url": "de.vtt", "source": "auto"}),
        ];
        let selected = select_caption_track(&tracks, "es", true).unwrap();
        assert_eq!(selected["locale_id"], "es_ES");
        assert!(select_caption_track(&tracks, "fr", true).is_none());
    }

    #[test]
    fn prefers_manual_captions_in_requested_language() {
        let tracks = vec![
            json!({"_class": "caption", "locale_id": "es_ES", "url": "auto.vtt", "source": "auto"}),
            json!({"_class": "caption", "locale_id": "es_MX", "url": "manual.vtt", "source": "upload"}),
        ];
        let selected = select_caption_track(&tracks, "es", true).unwrap();
        assert_eq!(selected["url"], "manual.vtt");
    }

    #[test]
    fn excludes_auto_captions_when_disabled() {
        let tracks = vec![
            json!({"_class": "caption", "locale_id": "es_ES", "url": "auto.vtt", "source": "auto"}),
            json!({"_class": "caption", "locale_id": "es_MX", "url": "manual.vtt", "source": "upload"}),
        ];
        let selected = select_caption_track(&tracks, "es", false).unwrap();
        assert_eq!(selected["url"], "manual.vtt");

        let auto_only = vec![tracks[0].clone()];
        assert!(select_caption_track(&auto_only, "es", false).is_none());
    }
}
