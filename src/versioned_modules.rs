use pyo3::{types::{PyModule, PyTuple}, FromPyObject, IntoPy, Py, PyResult, Python};
use semver::{Version, VersionReq};
use tracing::info;
use std::{collections::HashMap, fs, path::Path};
use tokio::sync::Semaphore;
use std::sync::{Arc, RwLock};
use rand::Rng;

// New struct to manage Python modules
pub struct VersionedModules {
    modules: RwLock<HashMap<String, HashMap<String, Vec<Py<PyModule>>>>>,
    latest_versions: RwLock<HashMap<String, String>>,
    pub interpreter_semaphore: Arc<Semaphore>,
    num_interpreters: usize,
}

impl VersionedModules {
    pub fn new(num_interpreters: usize) -> Self {
        Self {
            modules: RwLock::new(HashMap::new()),
            latest_versions: RwLock::new(HashMap::new()),
            interpreter_semaphore: Arc::new(Semaphore::new(num_interpreters)),
            num_interpreters,
        }
    }

    pub fn get_latest_version(&self, script_type: &str) -> Option<String> {
        self.latest_versions.read().unwrap().get(script_type).cloned()
    }

    pub fn contains_version(&self, script_type: &str, version: &str) -> bool {
        let modules = self.modules.read().unwrap();
        modules.get(script_type)
            .map(|versions| versions.contains_key(version))
            .unwrap_or(false)
    }

    pub fn load_modules(scripts_dir: &str, num_interpreters: usize) -> PyResult<Self> {
        info!("Initializing with {} interpreters", num_interpreters);
        let versioned_modules = Self::new(num_interpreters);
        versioned_modules.refresh_modules(scripts_dir)?;
        Ok(versioned_modules)
    }

    pub fn refresh_modules(&self, scripts_dir: &str) -> PyResult<()> {
        let scripts_path = Path::new(scripts_dir);
        info!("Loading scripts from: {}", scripts_path.display());

        // Read directory entries once
        let entries: Vec<_> = fs::read_dir(&scripts_path)
            .expect("Failed to read scripts directory")
            .collect::<Result<Vec<_>, _>>()
            .expect("Failed to read directory entries");

        // Updated regex to capture script type and version
        let script_pattern = regex::Regex::new(r"^([a-zA-Z_]+)_v(\d+(?:\.\d+)?(?:\.\d+)?)\.py$")
            .unwrap();

        let mut new_modules: HashMap<String, HashMap<String, Vec<Py<PyModule>>>> = HashMap::new();
        let mut script_versions: HashMap<String, Vec<String>> = HashMap::new();

        // Create modules for each interpreter
        for i in 0..self.num_interpreters {
            info!("Loading modules for interpreter {}", i);
            Python::with_gil(|py| -> PyResult<()> {
                for entry in &entries {  // Use the collected entries
                    let file_name = entry.file_name().to_string_lossy().to_string();

                    if let Some(captures) = script_pattern.captures(&file_name) {
                        let script_name = captures.get(1).unwrap().as_str().to_string();
                        let version = captures.get(2).unwrap().as_str().to_string();

                        script_versions.entry(script_name.clone())
                            .or_default()
                            .push(version.clone());

                        let script = fs::read_to_string(entry.path())
                            .expect("Failed to read Python script");

                        let module = PyModule::from_code(
                            py,
                            &script,
                            entry.path().to_str().unwrap(),
                            &format!("{}_v{}", script_name, version),
                        )?;

                        new_modules.entry(script_name)
                            .or_default()
                            .entry(version)
                            .or_insert_with(Vec::new)
                            .push(module.into());
                    }
                }
                Ok(())
            })?;
        }

        // Calculate latest versions
        let mut new_latest_versions = HashMap::new();
        for (script_name, versions) in script_versions {
            if !versions.is_empty() {
                let latest = versions.iter()
                    .filter_map(|v| Version::parse(v).ok())
                    .max()
                    .map(|v| v.to_string())
                    .unwrap();
                new_latest_versions.insert(script_name, latest);
            }
        }

        // Update state
        {
            let mut modules = self.modules.write().unwrap();
            let mut latest_versions = self.latest_versions.write().unwrap();
            *modules = new_modules;
            *latest_versions = new_latest_versions;
        }

        Ok(())
    }

    fn find_best_matching_version(&self, script_type: &str, requested: &str) -> Option<String> {
        let modules = self.modules.read().unwrap();
        let script_modules = modules.get(script_type)?;
        let binding = self.latest_versions.read().unwrap();
        let latest = binding.get(script_type)?;

        // Convert all available versions to semver::Version for comparison
        let versions: Vec<(String, Version)> = script_modules.keys()
            .filter_map(|v| Version::parse(v).ok().map(|parsed| (v.clone(), parsed)))
            .collect();

        info!("Looking for version matching: {}", requested);
        info!("Available versions: {:?}", versions.iter().map(|(s, _)| s).collect::<Vec<_>>());

        info!("Parsed versions: {:?}", versions.iter().map(|(s, v)| format!("{} -> {}", s, v)).collect::<Vec<_>>());

        // If just a major version is requested (e.g., "1"), return latest matching major
        if !requested.contains('.') {
            let major: u64 = requested.parse().unwrap_or(0);
            info!("Looking for latest version with major {}", major);
            return versions.iter()
                .filter(|(_, v)| v.major == major)
                .max_by(|(_, a), (_, b)| a.cmp(b))
                .map(|(s, _)| s.clone());
        }

        // Try exact match first
        if script_modules.contains_key(requested) {
            info!("Found exact match: {}", requested);
            return Some(requested.to_string());
        }

        // Use semver for compatibility matching
        if let Ok(req) = VersionReq::parse(&format!("^{}", requested)) {
            info!("Using version requirement: ^{}", requested);
            let matching = versions.iter()
                .filter(|(_, v)| req.matches(v))
                .max_by(|(_, a), (_, b)| a.cmp(b))
                .map(|(s, _)| s.clone());

            if let Some(version) = matching {
                info!("Found compatible version: {}", version);
                return Some(version);
            }
        }

        info!("No match found, using latest: {}", latest);
        Some(latest.clone())
    }

    pub fn get_module_with_version(&self, script_name: &str, requested: Option<String>) -> Option<(String, Vec<Py<PyModule>>)> {
        let version = match requested {
            Some(v) => self.find_best_matching_version(script_name, &v)?,
            None => self.get_latest_version(script_name)?,
        };

        let modules = self.modules.read().unwrap();
        let module_vec = modules.get(script_name)?.get(&version)?.clone();
        Some((version, module_vec))
    }

    pub async fn call_module_function<T: for<'a> FromPyObject<'a>>(
        modules: &[Py<PyModule>],
        function_name: &str,
        args: impl IntoPy<Py<PyTuple>>,
        semaphore: &Arc<Semaphore>,
    ) -> PyResult<T> {
        // Acquire an interpreter permit
        let _permit = semaphore.acquire().await.unwrap();

        // Get random interpreter index
        let interpreter_index = rand::thread_rng().gen_range(0..modules.len());

        // Use the selected interpreter
        Python::with_gil(|py| {
            modules[interpreter_index]
                .getattr(py, function_name)?
                .call1(py, args)?
                .extract(py)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_test_modules() -> VersionedModules {
        let modules = VersionedModules::new(1);  // Use single interpreter for tests

        Python::with_gil(|py| {
            // Create test modules for "one_hot" script type
            let mut module_map = HashMap::new();

            // Create Vec with single module for each version
            module_map.insert("1.0.0".to_string(), vec![PyModule::new(py, "test_one_hot").unwrap().into()]);
            module_map.insert("1.2.0".to_string(), vec![PyModule::new(py, "test_one_hot").unwrap().into()]);
            module_map.insert("1.3.0".to_string(), vec![PyModule::new(py, "test_one_hot").unwrap().into()]);

            // Create test modules for "feature_engineering" script type
            let mut fe_map = HashMap::new();
            fe_map.insert("2.0.0".to_string(), vec![PyModule::new(py, "test_fe").unwrap().into()]);
            fe_map.insert("2.1.0".to_string(), vec![PyModule::new(py, "test_fe").unwrap().into()]);

            // Insert both script types into modules
            modules.modules.write().unwrap().insert("one_hot".to_string(), module_map);
            modules.modules.write().unwrap().insert("feature_engineering".to_string(), fe_map);

            // Set latest versions for both script types
            let mut latest_versions = modules.latest_versions.write().unwrap();
            latest_versions.insert("one_hot".to_string(), "1.3.0".to_string());
            latest_versions.insert("feature_engineering".to_string(), "2.1.0".to_string());
        });

        modules
    }

    #[test]
    fn test_semver_matching() {
        let modules = setup_test_modules();

        // Test cases for "one_hot" script type
        let one_hot_cases = vec![
            ("1", "1.3.0"),     // Major version only -> get latest matching major
            ("1.2.0", "1.2.0"), // Exact match
            ("1.2", "1.2.0"),   // Major.minor match
            ("1.4.0", "1.3.0"), // Non-existent version -> get latest compatible
            ("2.0", "1.3.0"),   // Non-existent major -> get latest
        ];

        for (requested, expected) in one_hot_cases {
            let result = modules.find_best_matching_version("one_hot", requested);
            assert_eq!(result.as_deref(), Some(expected),
                "one_hot: Request for {} should return {}, got {:?}",
                requested, expected, result);
        }

        // Test cases for "feature_engineering" script type
        let fe_cases = vec![
            ("2", "2.1.0"),     // Major version only -> get latest matching major
            ("2.0.0", "2.0.0"), // Exact match
            ("2.1.0", "2.1.0"), // Exact match
            ("1.0.0", "2.1.0"), // Non-existent major -> get latest
        ];

        for (requested, expected) in fe_cases {
            let result = modules.find_best_matching_version("feature_engineering", requested);
            assert_eq!(result.as_deref(), Some(expected),
                "feature_engineering: Request for {} should return {}, got {:?}",
                requested, expected, result);
        }
    }

    #[test]
    fn test_get_latest_version() {
        let modules = setup_test_modules();

        assert_eq!(modules.get_latest_version("one_hot"), Some("1.3.0".to_string()));
        assert_eq!(modules.get_latest_version("feature_engineering"), Some("2.1.0".to_string()));
        assert_eq!(modules.get_latest_version("non_existent"), None);
    }

    #[test]
    fn test_contains_version() {
        let modules = setup_test_modules();

        assert!(modules.contains_version("one_hot", "1.3.0"));
        assert!(modules.contains_version("feature_engineering", "2.1.0"));
        assert!(!modules.contains_version("one_hot", "1.4.0"));
        assert!(!modules.contains_version("non_existent", "1.0.0"));
    }
}
