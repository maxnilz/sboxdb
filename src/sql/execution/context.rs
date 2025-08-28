use std::sync::Arc;

use crate::access::engine::Transaction;
use crate::sql::execution::compiler::RecordBatch;
use crate::sql::execution::compiler::RecordBatchesRef;

/// Context provides the execution environment and state for physical operators.
///
/// This trait abstracts the execution context to support both physical planning
/// and runtime execution phases with a unified interface. During planning, operators
/// can evaluate expr, e.g., column default expr, without accessing actual data, while
/// at runtime they can perform real computations with transaction state.
pub trait Context {
    /// Returns the transaction associated with this context.
    /// Used by physical operators to access catalog metadata and perform data operations.
    fn txn(&self) -> Arc<dyn Transaction>;

    /// Returns the configured batch size for vectorized execution.
    /// Physical operators use this to determine how many tuples to process at once.
    fn vector_size(&self) -> usize;

    /// Returns the current outer query record batches for correlated subquery execution.
    /// Used by subquery operators to access values from the outer query scope.
    fn outer_query_batches(&self) -> Option<RecordBatchesRef>;

    /// Extends the current outer query batches with a new record batch.
    /// Returns the previous batches that were replaced.
    /// Used when entering nested subquery scopes.
    fn extend_outer_query_batches(
        &mut self,
        rb: RecordBatch,
    ) -> crate::error::Result<Option<RecordBatchesRef>>;

    /// Replaces the current outer query batches with new ones.
    /// Returns the previous batches that were replaced.
    /// Used for managing subquery execution context.
    fn set_outer_query_batches(
        &mut self,
        batches: Option<RecordBatchesRef>,
    ) -> crate::error::Result<Option<RecordBatchesRef>>;
}

/// Const evaluation context used by the physical planning evaluation without dependencies
/// for actual data retrieval like catalog, transaction, etc. E.g., to evaluate the column
/// default value expr, the catalog/transaction is not needed.
pub struct ConstEvalCtx {}

impl ConstEvalCtx {
    pub fn new() -> Self {
        Self {}
    }
}

impl Context for ConstEvalCtx {
    fn txn(&self) -> Arc<dyn Transaction> {
        unimplemented!("ConstEvalCtx is only for physical planning use")
    }

    fn vector_size(&self) -> usize {
        unimplemented!("ConstEvalCtx is only for physical planning use")
    }

    fn outer_query_batches(&self) -> Option<RecordBatchesRef> {
        unimplemented!("ConstEvalCtx is only for physical planning use")
    }

    fn extend_outer_query_batches(
        &mut self,
        _rb: RecordBatch,
    ) -> crate::error::Result<Option<RecordBatchesRef>> {
        unimplemented!("ConstEvalCtx is only for physical planning use")
    }

    fn set_outer_query_batches(
        &mut self,
        _batches: Option<RecordBatchesRef>,
    ) -> crate::error::Result<Option<RecordBatchesRef>> {
        unimplemented!("ConstEvalCtx is only for physical planning use")
    }
}

/// Runtime execution context that can be passed over and across
/// different physical node during transaction execution.
#[derive(Clone)]
pub struct ExecContext {
    /// The transaction attached to the context
    transaction: Arc<dyn Transaction>,

    /// The record batches of the outer query, used to resolve
    /// the fields in subquery.
    outer_query_batches: Option<RecordBatchesRef>,

    // TODO: pick proper batch size with respect to the output tuple
    //  size and cache line size and try to arrange the output tuples
    //  layout being cache friendly.
    vector_size: usize,
}

impl ExecContext {
    pub fn new(transaction: Arc<dyn Transaction>, vector_size: usize) -> Self {
        Self { transaction, outer_query_batches: None, vector_size }
    }
}
impl Context for ExecContext {
    fn txn(&self) -> Arc<dyn Transaction> {
        self.transaction.clone()
    }

    fn vector_size(&self) -> usize {
        self.vector_size
    }

    fn outer_query_batches(&self) -> Option<RecordBatchesRef> {
        self.outer_query_batches.clone()
    }

    fn extend_outer_query_batches(
        &mut self,
        rb: RecordBatch,
    ) -> crate::error::Result<Option<RecordBatchesRef>> {
        let prev = self.outer_query_batches.clone();
        match self.outer_query_batches.as_mut() {
            None => self.outer_query_batches = Some(RecordBatchesRef::new(rb)),
            Some(batches) => batches.push(rb)?,
        };
        Ok(prev)
    }

    fn set_outer_query_batches(
        &mut self,
        mut batches: Option<RecordBatchesRef>,
    ) -> crate::error::Result<Option<RecordBatchesRef>> {
        std::mem::swap(&mut self.outer_query_batches, &mut batches);
        Ok(batches)
    }
}
