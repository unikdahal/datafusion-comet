// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Native Iceberg orphan-file removal.
//!
//! The JVM is only the control-plane/JNI boundary. Metadata parsing, reachable-file discovery,
//! object-store listing, age filtering, path comparison, and deletion are performed here.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use iceberg::io::{FileIO, FileIOBuilder, StorageFactory};
use iceberg::spec::{Manifest, ManifestList, TableMetadata};
use iceberg_storage_opendal::OpenDalStorageFactory;
use jni::objects::{JClass, JMap, JObject, JString};
use jni::sys::{jint, jlong, jobjectArray};
use jni::{Env, EnvUnowned};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use url::Url;

use crate::errors::{try_unwrap_or_throw, CometError};
use crate::execution::jni_api::get_runtime;
use crate::execution::operators::ExecutionError;
use crate::parquet::parquet_support::prepare_object_store_with_configs;

fn java_map_to_hashmap(
    env: &mut Env,
    map_object: JObject,
) -> Result<HashMap<String, String>, CometError> {
    let map = env.cast_local::<JMap>(map_object)?;
    let mut collected = HashMap::new();
    map.iter(env).and_then(|mut iter| {
        while let Some(entry) = iter.next(env)? {
            let key = entry.key(env)?;
            let value = entry.value(env)?;
            let key = unsafe { JString::from_raw(env, key.into_raw()) };
            let value = unsafe { JString::from_raw(env, value.into_raw()) };
            collected.insert(key.try_to_string(env)?, value.try_to_string(env)?);
        }
        Ok(())
    })?;
    Ok(collected)
}

const MIN_RETENTION_MS: i64 = 24 * 60 * 60 * 1000;
const VERSION_HINT_FILE: &str = "version-hint.text";
const ENCRYPTION_KEY_ID_PROPERTY: &str = "encryption.key-id";
const FALLBACK_MARKER: &str = "[COMET_ICEBERG_FALLBACK]";
const STORAGE_PROPERTY_PREFIXES: &[&str] = &["s3.", "client."];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PrefixMismatchMode {
    Error,
    Ignore,
    Delete,
}

impl PrefixMismatchMode {
    fn parse(value: &str) -> Result<Self, ExecutionError> {
        match value.trim().to_ascii_uppercase().as_str() {
            "ERROR" => Ok(Self::Error),
            "IGNORE" => Ok(Self::Ignore),
            "DELETE" => Ok(Self::Delete),
            other => Err(ExecutionError::GeneralError(format!(
                "Invalid prefix_mismatch_mode '{other}'; expected ERROR, IGNORE, or DELETE"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileIdentity {
    scheme: Option<String>,
    authority: Option<String>,
    path: String,
}

impl FileIdentity {
    fn parse(
        raw: &str,
        equal_schemes: &HashMap<String, String>,
        equal_authorities: &HashMap<String, String>,
    ) -> Self {
        if let Ok(url) = Url::parse(raw) {
            Self {
                scheme: normalize_component(Some(url.scheme()), equal_schemes),
                authority: normalize_authority(&url, equal_authorities),
                path: url.path().to_string(),
            }
        } else {
            Self {
                scheme: None,
                authority: None,
                path: raw.to_string(),
            }
        }
    }

    fn prefix_matches(&self, actual: &Self) -> bool {
        component_matches(self.scheme.as_deref(), actual.scheme.as_deref())
            && component_matches(self.authority.as_deref(), actual.authority.as_deref())
    }
}

fn component_matches(valid: Option<&str>, actual: Option<&str>) -> bool {
    valid
        .filter(|value| !value.is_empty())
        .map(|valid| {
            actual
                .map(|actual| valid.eq_ignore_ascii_case(actual))
                .unwrap_or(false)
        })
        .unwrap_or(true)
}

fn normalize_component(
    value: Option<&str>,
    equivalences: &HashMap<String, String>,
) -> Option<String> {
    value.map(|value| {
        let key = value.to_ascii_lowercase();
        equivalences
            .get(&key)
            .cloned()
            .unwrap_or(key)
            .to_ascii_lowercase()
    })
}

fn normalize_authority(url: &Url, equivalences: &HashMap<String, String>) -> Option<String> {
    let authority = match (url.host_str(), url.port()) {
        (Some(host), Some(port)) => Some(format!("{host}:{port}")),
        (Some(host), None) => Some(host.to_string()),
        (None, _) => None,
    };
    normalize_component(authority.as_deref(), equivalences)
}

fn flatten_equivalences(
    custom: HashMap<String, String>,
    include_default_s3_schemes: bool,
) -> HashMap<String, String> {
    let mut flattened = HashMap::new();
    if include_default_s3_schemes {
        flattened.insert("s3a".to_string(), "s3".to_string());
        flattened.insert("s3n".to_string(), "s3".to_string());
    }
    for (keys, value) in custom {
        let canonical = value.trim().to_ascii_lowercase();
        for key in keys.split(',') {
            let key = key.trim();
            if !key.is_empty() {
                flattened.insert(key.to_ascii_lowercase(), canonical.clone());
            }
        }
    }
    flattened
}

fn current_time_ms() -> Result<i64, ExecutionError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| {
            ExecutionError::GeneralError(format!("System clock is before UNIX epoch: {e}"))
        })?
        .as_millis();
    i64::try_from(millis).map_err(|_| {
        ExecutionError::GeneralError(
            "Current timestamp does not fit in signed 64-bit milliseconds".to_string(),
        )
    })
}

fn validate_retention(older_than_ms: i64, allow_unsafe: bool) -> Result<(), ExecutionError> {
    if allow_unsafe {
        return Ok(());
    }
    // Negative cutoffs (pre-epoch TIMESTAMPs) yield a large interval and pass. This mirrors
    // Iceberg-Java which only enforces the 24-hour safety interval.
    let interval = current_time_ms()?.saturating_sub(older_than_ms);
    if interval < MIN_RETENTION_MS {
        return Err(ExecutionError::GeneralError(
            "Cannot remove orphan files with an interval less than 24 hours. A shorter interval \
             may delete files from concurrent table operations."
                .to_string(),
        ));
    }
    Ok(())
}

fn exec_error(context: impl AsRef<str>, err: impl std::fmt::Display) -> ExecutionError {
    ExecutionError::GeneralError(format!("{}: {err}", context.as_ref()))
}

fn fallback_error(message: impl AsRef<str>) -> ExecutionError {
    ExecutionError::GeneralError(format!("{FALLBACK_MARKER} {}", message.as_ref()))
}

fn storage_factory_for(location: &str) -> Result<Arc<dyn StorageFactory>, ExecutionError> {
    let scheme = if location.contains("://") {
        location.split("://").next().unwrap_or("file")
    } else {
        "file"
    };
    match scheme {
        "file" => Ok(Arc::new(OpenDalStorageFactory::Fs)),
        "s3" | "s3a" | "s3n" => Ok(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        })),
        other => Err(fallback_error(format!(
            "Unsupported Iceberg metadata storage scheme '{other}' for native remove_orphan_files"
        ))),
    }
}

fn s3_file_io_props_from_object_store(
    metadata_location: &str,
    object_store_options: &HashMap<String, String>,
) -> HashMap<String, String> {
    let bucket = Url::parse(metadata_location)
        .ok()
        .and_then(|url| url.host_str().map(str::to_string));
    let mappings = [
        ("access.key", "s3.access-key-id"),
        ("secret.key", "s3.secret-access-key"),
        ("session.token", "s3.session-token"),
        ("endpoint", "s3.endpoint"),
        ("path.style.access", "s3.path-style-access"),
        ("endpoint.region", "s3.region"),
    ];

    mappings
        .into_iter()
        .filter_map(|(suffix, target)| {
            let bucket_value = bucket.as_ref().and_then(|bucket| {
                object_store_options.get(&format!("fs.s3a.bucket.{bucket}.{suffix}"))
            });
            bucket_value
                .or_else(|| object_store_options.get(&format!("fs.s3a.{suffix}")))
                .map(|value| (target.to_string(), value.clone()))
        })
        .collect()
}

fn build_file_io(
    metadata_location: &str,
    file_io_properties: &HashMap<String, String>,
    object_store_options: &HashMap<String, String>,
) -> Result<FileIO, ExecutionError> {
    let factory = storage_factory_for(metadata_location)?;
    let mut props = file_io_properties.clone();
    for (key, value) in s3_file_io_props_from_object_store(metadata_location, object_store_options)
    {
        props.entry(key).or_insert(value);
    }

    let mut builder = FileIOBuilder::new(factory);
    for (key, value) in props {
        if STORAGE_PROPERTY_PREFIXES
            .iter()
            .any(|prefix| key.starts_with(prefix))
        {
            builder = builder.with_prop(key, value);
        }
    }
    Ok(builder.build())
}

#[derive(Clone)]
struct StoreResolver {
    runtime_env: Arc<RuntimeEnv>,
    object_store_options: HashMap<String, String>,
    file_io: FileIO,
}

impl StoreResolver {
    fn new(
        metadata_location: &str,
        file_io_properties: HashMap<String, String>,
        object_store_options: HashMap<String, String>,
    ) -> Result<Self, ExecutionError> {
        let ctx = SessionContext::new();
        let file_io = build_file_io(
            metadata_location,
            &file_io_properties,
            &object_store_options,
        )?;
        Ok(Self {
            runtime_env: ctx.runtime_env(),
            object_store_options,
            file_io,
        })
    }

    fn resolve(&self, location: &str) -> Result<(Arc<dyn ObjectStore>, Path), ExecutionError> {
        let (store_url, path) = prepare_object_store_with_configs(
            Arc::clone(&self.runtime_env),
            location.to_string(),
            &self.object_store_options,
        )?;
        let store = self.runtime_env.object_store(&store_url).map_err(|e| {
            exec_error(
                format!("Failed to resolve object store for '{location}'"),
                e,
            )
        })?;
        Ok((store, path))
    }

    fn read(&self, location: &str) -> Result<Vec<u8>, ExecutionError> {
        let input = self
            .file_io
            .new_input(location)
            .map_err(|e| exec_error(format!("Failed to open '{location}'"), e))?;
        let location = location.to_string();
        get_runtime().block_on(async move {
            input
                .read()
                .await
                .map(|bytes| bytes.to_vec())
                .map_err(|e| exec_error(format!("Failed to read '{location}'"), e))
        })
    }

    fn read_metadata(&self, location: &str) -> Result<TableMetadata, ExecutionError> {
        get_runtime()
            .block_on(TableMetadata::read_from(&self.file_io, location))
            .map_err(|e| exec_error(format!("Failed to read Iceberg metadata '{location}'"), e))
    }

    fn read_optional_metadata(&self, location: &str) -> Option<TableMetadata> {
        match self.read_metadata(location) {
            Ok(metadata) => Some(metadata),
            Err(err) => {
                log::warn!("Unable to read previous Iceberg metadata '{location}': {err}");
                None
            }
        }
    }
}

fn version_hint_location(table_location: &str) -> String {
    format!(
        "{}/metadata/{VERSION_HINT_FILE}",
        table_location.trim_end_matches('/')
    )
}

fn collect_metadata_files(
    resolver: &StoreResolver,
    current_location: &str,
    current: &TableMetadata,
    reachable: &mut HashSet<String>,
) {
    reachable.insert(current_location.to_string());
    let mut queued = HashSet::new();
    let mut queue = VecDeque::new();

    for entry in current.metadata_log() {
        reachable.insert(entry.metadata_file.clone());
        if queued.insert(entry.metadata_file.clone()) {
            queue.push_back(entry.metadata_file.clone());
        }
    }

    // Conservative metadata recursion: preserving extra historical JSON is safe, while failing to
    // preserve a still-useful metadata file is not. Content files from previous metadata are not
    // added; only the metadata-log chain itself is retained.
    while let Some(location) = queue.pop_front() {
        let Some(metadata) = resolver.read_optional_metadata(&location) else {
            continue;
        };
        for entry in metadata.metadata_log() {
            reachable.insert(entry.metadata_file.clone());
            if queued.insert(entry.metadata_file.clone()) {
                queue.push_back(entry.metadata_file.clone());
            }
        }
    }
}

fn collect_reachable_files(
    resolver: &StoreResolver,
    metadata_location: &str,
) -> Result<HashSet<String>, ExecutionError> {
    // TableMetadata::read_from handles Iceberg's metadata JSON compression transparently.
    let metadata = resolver.read_metadata(metadata_location)?;

    if metadata
        .properties()
        .contains_key(ENCRYPTION_KEY_ID_PROPERTY)
    {
        return Err(fallback_error(
            "Native remove_orphan_files does not yet support encrypted Iceberg metadata/manifests",
        ));
    }

    let gc_enabled = metadata
        .table_properties()
        .map(|props| props.gc_enabled)
        .map_err(|e| exec_error("Invalid Iceberg gc.enabled property", e))?;
    if !gc_enabled {
        return Err(ExecutionError::GeneralError(
            "Cannot delete orphan files: gc.enabled is false (deleting files may corrupt other tables)"
                .to_string(),
        ));
    }

    let mut reachable = HashSet::new();
    collect_metadata_files(resolver, metadata_location, &metadata, &mut reachable);
    reachable.insert(version_hint_location(metadata.location()));

    for stats in metadata.statistics_iter() {
        reachable.insert(stats.statistics_path.clone());
    }
    for stats in metadata.partition_statistics_iter() {
        reachable.insert(stats.statistics_path.clone());
    }

    let mut manifest_paths = HashSet::new();
    for snapshot in metadata.snapshots() {
        let manifest_list_path = snapshot.manifest_list();
        if manifest_list_path.is_empty() {
            continue;
        }
        reachable.insert(manifest_list_path.to_string());
        let bytes = resolver.read(manifest_list_path)?;
        let manifest_list = ManifestList::parse_with_version(&bytes, metadata.format_version())
            .map_err(|e| {
                exec_error(
                    format!("Failed to parse manifest list '{manifest_list_path}'"),
                    e,
                )
            })?;
        for manifest_file in manifest_list.entries() {
            reachable.insert(manifest_file.manifest_path.clone());
            manifest_paths.insert(manifest_file.manifest_path.clone());
        }
    }

    for manifest_path in manifest_paths {
        let bytes = resolver.read(&manifest_path)?;
        let manifest = Manifest::parse_avro(&bytes)
            .map_err(|e| exec_error(format!("Failed to parse manifest '{manifest_path}'"), e))?;
        for entry in manifest.entries() {
            reachable.insert(entry.data_file().file_path().to_string());
        }
    }

    Ok(reachable)
}

fn materialize_location(base: &str, object_path: &Path) -> Result<String, ExecutionError> {
    if let Ok(mut url) = Url::parse(base) {
        let path = object_path.as_ref();
        if path.starts_with('/') {
            url.set_path(path);
        } else {
            url.set_path(&format!("/{path}"));
        }
        url.set_query(None);
        url.set_fragment(None);
        return Ok(url.to_string());
    }

    // Bare paths (no scheme) cannot be compared safely against metadata URIs. Failing here
    // triggers fallback to Iceberg-Java instead of misclassifying every file as an orphan.
    Err(fallback_error(format!(
        "Unsupported scan location '{base}' for native remove_orphan_files"
    )))
}

fn build_valid_by_path(
    reachable: HashSet<String>,
    equal_schemes: &HashMap<String, String>,
    equal_authorities: &HashMap<String, String>,
) -> HashMap<String, Vec<FileIdentity>> {
    let mut valid_by_path: HashMap<String, Vec<FileIdentity>> = HashMap::new();
    for location in reachable {
        let ident = FileIdentity::parse(&location, equal_schemes, equal_authorities);
        valid_by_path
            .entry(ident.path.clone())
            .or_default()
            .push(ident);
    }
    valid_by_path
}

fn classify_candidate(
    location: &str,
    valid_by_path: &HashMap<String, Vec<FileIdentity>>,
    equal_schemes: &HashMap<String, String>,
    equal_authorities: &HashMap<String, String>,
    mode: PrefixMismatchMode,
    conflicts: &mut HashSet<String>,
) -> bool {
    let actual_ident = FileIdentity::parse(location, equal_schemes, equal_authorities);
    let Some(valid) = valid_by_path.get(&actual_ident.path) else {
        return true;
    };

    if valid
        .iter()
        .any(|candidate| candidate.prefix_matches(&actual_ident))
    {
        return false;
    }

    match mode {
        PrefixMismatchMode::Delete => true,
        PrefixMismatchMode::Ignore => false,
        PrefixMismatchMode::Error => {
            for candidate in valid {
                if !component_matches(
                    candidate.scheme.as_deref(),
                    actual_ident.scheme.as_deref(),
                ) {
                    conflicts.insert(format!(
                        "scheme {:?} vs {:?}",
                        candidate.scheme, actual_ident.scheme
                    ));
                }
                if !component_matches(
                    candidate.authority.as_deref(),
                    actual_ident.authority.as_deref(),
                ) {
                    conflicts.insert(format!(
                        "authority {:?} vs {:?}",
                        candidate.authority, actual_ident.authority
                    ));
                }
            }
            false
        }
    }
}

fn validate_prefix_conflicts(conflicts: HashSet<String>) -> Result<(), ExecutionError> {
    if conflicts.is_empty() {
        return Ok(());
    }

    let mut conflicts: Vec<_> = conflicts.into_iter().collect();
    conflicts.sort();
    Err(ExecutionError::GeneralError(format!(
        "Unable to determine whether certain files are orphan because metadata and listed files \
         have matching paths but conflicting schemes/authorities: {}. Configure \
         equal_schemes/equal_authorities, use IGNORE, or use DELETE only after verifying the \
         remaining prefixes refer to different storage.",
        conflicts.join(", ")
    )))
}

fn find_orphans(
    actual: Vec<String>,
    reachable: HashSet<String>,
    equal_schemes: &HashMap<String, String>,
    equal_authorities: &HashMap<String, String>,
    mode: PrefixMismatchMode,
) -> Result<Vec<String>, ExecutionError> {
    let valid_by_path = build_valid_by_path(reachable, equal_schemes, equal_authorities);
    let mut conflicts = HashSet::new();
    let mut orphans = Vec::new();

    for location in actual {
        if classify_candidate(
            &location,
            &valid_by_path,
            equal_schemes,
            equal_authorities,
            mode,
            &mut conflicts,
        ) {
            orphans.push(location);
        }
    }

    validate_prefix_conflicts(conflicts)?;
    orphans.sort();
    Ok(orphans)
}

async fn delete_paths_with<F, Fut>(
    paths: Vec<Path>,
    max_concurrent_deletes: usize,
    delete: F,
) where
    F: Fn(Path) -> Fut,
    Fut: std::future::Future<Output = Result<(), ExecutionError>>,
{
    let delete = Arc::new(delete);
    let mut pending = futures::stream::iter(paths.into_iter().map(|path| {
        let delete = Arc::clone(&delete);
        async move {
            let display_path = path.to_string();
            if let Err(err) = delete(path).await {
                // Match Iceberg's non-bulk maintenance semantics: individual delete failures are
                // logged and the remaining orphan files are still attempted. The operation is
                // already non-transactional, so fail-fast would only make partial progress harder
                // to reason about and would diverge from the established procedure behavior.
                log::warn!("Failed to delete orphan file '{display_path}': {err}");
            }
        }
    }))
    .buffer_unordered(max_concurrent_deletes);

    while pending.next().await.is_some() {}
}

struct RemoveOrphansArgs {
    metadata_location: String,
    scan_location: String,
    older_than_ms: i64,
    dry_run: bool,
    max_concurrent_deletes: usize,
    file_io_properties: HashMap<String, String>,
    object_store_options: HashMap<String, String>,
    equal_schemes: HashMap<String, String>,
    equal_authorities: HashMap<String, String>,
    prefix_mismatch_mode: PrefixMismatchMode,
    allow_unsafe_older_than: bool,
}

fn execute_remove_orphans(args: RemoveOrphansArgs) -> Result<Vec<String>, ExecutionError> {
    validate_retention(args.older_than_ms, args.allow_unsafe_older_than)?;
    if args.max_concurrent_deletes == 0 {
        return Err(ExecutionError::GeneralError(
            "max_concurrent_deletes must be greater than 0".to_string(),
        ));
    }

    let equal_schemes = flatten_equivalences(args.equal_schemes, true);
    let equal_authorities = flatten_equivalences(args.equal_authorities, false);
    let resolver = StoreResolver::new(
        &args.metadata_location,
        args.file_io_properties,
        args.object_store_options,
    )?;
    let reachable = collect_reachable_files(&resolver, &args.metadata_location)?;
    let valid_by_path = build_valid_by_path(reachable, &equal_schemes, &equal_authorities);

    // Resolve the listing store before entering the async block. S3 object-store construction may
    // itself use Comet's Tokio runtime for region/credential resolution.
    let (store, prefix) = resolver.resolve(&args.scan_location)?;
    let scan_location_for_listing = args.scan_location.clone();
    let older_than_ms = args.older_than_ms;
    let prefix_mismatch_mode = args.prefix_mismatch_mode;

    // Classify candidates as they are listed. This keeps memory proportional to reachable metadata
    // plus actual orphan files rather than materializing every old object under the scan prefix and
    // then cloning those paths into additional comparison collections.
    let (mut orphan_candidates, conflicts) = get_runtime().block_on(async {
        let mut stream = store.list(Some(&prefix));
        let mut orphan_candidates = Vec::new();
        let mut conflicts = HashSet::new();
        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(|e| {
                exec_error(
                    format!("Failed while listing '{scan_location_for_listing}'"),
                    e,
                )
            })?;
            if meta.last_modified.timestamp_millis() < older_than_ms {
                let location = materialize_location(&scan_location_for_listing, &meta.location)?;
                if classify_candidate(
                    &location,
                    &valid_by_path,
                    &equal_schemes,
                    &equal_authorities,
                    prefix_mismatch_mode,
                    &mut conflicts,
                ) {
                    orphan_candidates.push((location, meta.location));
                }
            }
        }
        Ok::<(Vec<(String, Path)>, HashSet<String>), ExecutionError>((
            orphan_candidates,
            conflicts,
        ))
    })?;

    // All validation and mismatch detection is complete before this point. In particular, ERROR
    // mismatch mode can never fail after some deletes have already happened.
    validate_prefix_conflicts(conflicts)?;
    orphan_candidates.sort_by(|left, right| left.0.cmp(&right.0));

    if args.dry_run || orphan_candidates.is_empty() {
        return Ok(orphan_candidates
            .into_iter()
            .map(|(location, _)| location)
            .collect());
    }

    let delete_paths: Vec<Path> = orphan_candidates
        .iter()
        .map(|(_, path)| path.clone())
        .collect();
    let store_for_delete = Arc::clone(&store);
    get_runtime().block_on(delete_paths_with(
        delete_paths,
        args.max_concurrent_deletes,
        move |path| {
            let store = Arc::clone(&store_for_delete);
            async move {
                store
                    .delete(&path)
                    .await
                    .map_err(|e| exec_error(format!("Failed to delete '{path}'"), e))
            }
        },
    ));

    Ok(orphan_candidates
        .into_iter()
        .map(|(location, _)| location)
        .collect())
}

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_iceberg_NativeIcebergMaintenance_removeOrphanFiles(
    env: EnvUnowned,
    _class: JClass,
    metadata_location: JString,
    scan_location: JString,
    older_than_ms: jlong,
    max_concurrent_deletes: jint,
    file_io_properties: JObject,
    object_store_options: JObject,
    equal_schemes: JObject,
    equal_authorities: JObject,
    prefix_mismatch_mode: JString,
    flags: jint,
) -> jobjectArray {
    // flags bit 0 = dry run, bit 1 = allow unsafe older_than interval.
    const FLAG_DRY_RUN: jint = 1;
    const FLAG_ALLOW_UNSAFE: jint = 2;
    try_unwrap_or_throw(&env, |env| {
        let metadata_location: String = metadata_location.try_to_string(env)?;
        let scan_location: String = scan_location.try_to_string(env)?;
        let file_io_properties = java_map_to_hashmap(env, file_io_properties)?;
        let object_store_options = java_map_to_hashmap(env, object_store_options)?;
        let equal_schemes = java_map_to_hashmap(env, equal_schemes)?;
        let equal_authorities = java_map_to_hashmap(env, equal_authorities)?;
        let prefix_mismatch_mode: String = prefix_mismatch_mode.try_to_string(env)?;
        let prefix_mismatch_mode = PrefixMismatchMode::parse(&prefix_mismatch_mode)?;

        if max_concurrent_deletes <= 0 {
            return Err(CometError::Execution {
                source: ExecutionError::GeneralError(format!(
                    "max_concurrent_deletes must be greater than 0, value: {max_concurrent_deletes}"
                )),
            });
        }

        let orphan_files = execute_remove_orphans(RemoveOrphansArgs {
            metadata_location,
            scan_location,
            older_than_ms,
            dry_run: flags & FLAG_DRY_RUN != 0,
            max_concurrent_deletes: max_concurrent_deletes as usize,
            file_io_properties,
            object_store_options,
            equal_schemes,
            equal_authorities,
            prefix_mismatch_mode,
            allow_unsafe_older_than: flags & FLAG_ALLOW_UNSAFE != 0,
        })?;

        let string_class = env.find_class(jni::jni_str!("java/lang/String"))?;
        let result =
            env.new_object_array(orphan_files.len() as jint, string_class, JObject::null())?;
        for (idx, location) in orphan_files.iter().enumerate() {
            let value = env.new_string(location)?;
            result.set_element(env, idx, &value)?;
        }
        Ok(result.into_raw())
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[test]
    fn default_s3_schemes_are_equivalent() {
        let schemes = flatten_equivalences(HashMap::new(), true);
        let a = FileIdentity::parse("s3a://bucket/path/file.parquet", &schemes, &HashMap::new());
        let b = FileIdentity::parse("s3://bucket/path/file.parquet", &schemes, &HashMap::new());
        assert_eq!(a.path, b.path);
        assert!(a.prefix_matches(&b));
    }

    #[test]
    fn custom_equivalence_keys_are_flattened() {
        let schemes = flatten_equivalences(
            HashMap::from([("s3a,s3n".to_string(), "s3".to_string())]),
            false,
        );
        assert_eq!(schemes.get("s3a").map(String::as_str), Some("s3"));
        assert_eq!(schemes.get("s3n").map(String::as_str), Some("s3"));
    }

    #[test]
    fn exact_reachable_file_is_not_orphan() {
        let result = find_orphans(
            vec!["s3://bucket/data/a.parquet".to_string()],
            HashSet::from(["s3://bucket/data/a.parquet".to_string()]),
            &flatten_equivalences(HashMap::new(), true),
            &HashMap::new(),
            PrefixMismatchMode::Error,
        )
        .unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn unreferenced_file_is_orphan() {
        let actual = vec!["s3://bucket/data/orphan.parquet".to_string()];
        let result = find_orphans(
            actual.clone(),
            HashSet::new(),
            &flatten_equivalences(HashMap::new(), true),
            &HashMap::new(),
            PrefixMismatchMode::Error,
        )
        .unwrap();
        assert_eq!(result, actual);
    }

    #[test]
    fn mismatch_error_fails_before_deletion() {
        let err = find_orphans(
            vec!["s3://actual/data/a.parquet".to_string()],
            HashSet::from(["s3://valid/data/a.parquet".to_string()]),
            &flatten_equivalences(HashMap::new(), true),
            &HashMap::new(),
            PrefixMismatchMode::Error,
        )
        .unwrap_err();
        assert!(err.to_string().contains("conflicting schemes/authorities"));
    }

    #[test]
    fn mismatch_ignore_preserves_file() {
        let result = find_orphans(
            vec!["s3://actual/data/a.parquet".to_string()],
            HashSet::from(["s3://valid/data/a.parquet".to_string()]),
            &flatten_equivalences(HashMap::new(), true),
            &HashMap::new(),
            PrefixMismatchMode::Ignore,
        )
        .unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn mismatch_delete_marks_file_orphan() {
        let actual = vec!["s3://actual/data/a.parquet".to_string()];
        let result = find_orphans(
            actual.clone(),
            HashSet::from(["s3://valid/data/a.parquet".to_string()]),
            &flatten_equivalences(HashMap::new(), true),
            &HashMap::new(),
            PrefixMismatchMode::Delete,
        )
        .unwrap();
        assert_eq!(result, actual);
    }

    #[test]
    fn negative_cutoff_is_allowed_and_passes_retention() {
        // Pre-epoch TIMESTAMPs are valid Iceberg cutoffs with a large interval.
        assert!(validate_retention(-1, true).is_ok());
        assert!(validate_retention(-315619200000, false).is_ok());
    }

    #[test]
    fn bare_scan_location_uses_structured_fallback_marker() {
        let err = materialize_location(
            "/tmp/warehouse",
            &object_store::path::Path::from("data/a.parquet"),
        )
        .unwrap_err();
        assert!(err.to_string().contains(FALLBACK_MARKER));
    }

    #[test]
    fn unsupported_storage_uses_structured_fallback_marker() {
        let err = storage_factory_for("gs://bucket/table/metadata/v1.json").unwrap_err();
        assert!(err.to_string().contains(FALLBACK_MARKER));
    }

    #[test]
    fn delete_failures_do_not_stop_remaining_deletes() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let failed = Arc::new(AtomicUsize::new(0));
        let paths = vec![Path::from("a"), Path::from("b"), Path::from("c")];
        let attempts_for_delete = Arc::clone(&attempts);
        let failed_for_delete = Arc::clone(&failed);

        get_runtime().block_on(delete_paths_with(paths, 2, move |path| {
            let attempts = Arc::clone(&attempts_for_delete);
            let failed = Arc::clone(&failed_for_delete);
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                if path.to_string() == "b" {
                    failed.fetch_add(1, Ordering::SeqCst);
                    Err(ExecutionError::GeneralError("injected delete failure".to_string()))
                } else {
                    Ok(())
                }
            }
        }));

        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert_eq!(failed.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn version_hint_uses_table_root_layout() {
        assert_eq!(
            version_hint_location("file:///warehouse/db/table/"),
            "file:///warehouse/db/table/metadata/version-hint.text"
        );
    }
}