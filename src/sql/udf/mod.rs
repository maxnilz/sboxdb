use std::collections::HashMap;
use std::sync::Arc;
use std::sync::LazyLock;

use crate::sql::udf::aggregate::count;
use crate::sql::udf::aggregate::AggregateUDF;
use crate::sql::udf::scalar::upper;
use crate::sql::udf::scalar::ScalarUDF;

pub mod aggregate;
pub mod scalar;
pub mod signature;

/// User defined functions registry
pub trait FuncRegistry: Send + Sync {
    /// Get scalar udf with the given name.
    fn udf(&self, name: &str) -> Option<Arc<dyn ScalarUDF>>;

    /// Get aggregate udf with the given name.
    fn udaf(&self, name: &str) -> Option<Arc<dyn AggregateUDF>>;
}

pub fn new_func_registry() -> Arc<dyn FuncRegistry> {
    static INSTANCE: LazyLock<Arc<dyn FuncRegistry>> =
        LazyLock::new(|| Arc::new(BuiltinFuncRegistry::new()));
    Arc::clone(&INSTANCE)
}

struct BuiltinFuncRegistry {
    scalar_functions: HashMap<String, Arc<dyn ScalarUDF>>,
    aggregate_functions: HashMap<String, Arc<dyn AggregateUDF>>,
}

impl BuiltinFuncRegistry {
    fn new() -> Self {
        let scalar_functions = vec![upper()];
        let aggregate_functions = vec![count()];
        let scalar_map = scalar_functions
            .into_iter()
            .map(|it| (it.name().to_string(), it))
            .collect::<HashMap<String, Arc<dyn ScalarUDF>>>();
        let aggregate_map = aggregate_functions
            .into_iter()
            .map(|it| (it.name().to_string(), it))
            .collect::<HashMap<String, Arc<dyn AggregateUDF>>>();
        Self { scalar_functions: scalar_map, aggregate_functions: aggregate_map }
    }
}

impl FuncRegistry for BuiltinFuncRegistry {
    fn udf(&self, name: &str) -> Option<Arc<dyn ScalarUDF>> {
        self.scalar_functions.get(name).cloned()
    }

    fn udaf(&self, name: &str) -> Option<Arc<dyn AggregateUDF>> {
        self.aggregate_functions.get(name).cloned()
    }
}
