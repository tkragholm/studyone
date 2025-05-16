/// Get all available year files from a registry directory
#[must_use]
pub fn get_available_year_files(registry: &str) -> Vec<PathBuf> {
    let dir = registry_dir(registry);
    if !dir.exists() {
        return Vec::new();
    }

    std::fs::read_dir(dir)
        .ok()
        .map(|entries| {
            entries
                .filter_map(|res: std::io::Result<std::fs::DirEntry>| res.ok())
                .filter(|entry| {
                    let path = entry.path();
                    path.is_file()
                        && path.extension().is_some_and(|ext| ext == "parquet")
                        && path
                            .file_stem()
                            .is_some_and(|name| name.to_string_lossy().parse::<u32>().is_ok())
                })
                .map(|entry| entry.path())
                .collect()
        })
        .unwrap_or_default()
}
