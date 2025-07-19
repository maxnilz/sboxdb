mod expr;
mod plan;
mod planner;
mod schema;

// datafusion code path
//
// SessionState::sql_with_options
//   SessionState::create_logical_plan
//     SqlToRel::sql_statement_to_plan
//     SqlToRel::sql_to_expr -> sql_expr_to_logical_expr -> sql_expr_to_logical_expr_internal
//     SqlToRel::select_to_plan
//     SqlToRel::plan_from_tables -> plan_table_with_joins -> create_relation/parse_relation_join -> parse_join
//  SessionState::execute_logical_plan
// DataFrame::new
// DataFrame::collect
// DataFrame::create_physical_plan
//   SessionState::create_physical_plan
//     QueryPlanner::create_physical_plan
//       DefaultQueryPlanner::create_physical_plan
//         DefaultPhysicalPlanner::create_physical_plan
//           DefaultPhysicalPlanner::handle_explain_or_analyze
//           DefaultPhysicalPlanner::create_initial_plan
//           DefaultPhysicalPlanner::optimize_physical_plan
// execution_plan::collect
// execution_plan::execute_stream
// common::collect(stream)
