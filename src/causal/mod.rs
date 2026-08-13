//! Causal annotations for DSRV stream values.
//!
//! [`CausalSet`] is the default reference domain: it records the external
//! observations supporting a result without classifying them.  The
//! role-aware [`RoleCausalSet`] and [`RoleCausalAntichain`] domains refine
//! those occurrences with [`CausalRole`] annotations.

mod atom;
mod causal_set;
mod domain;
mod explanation;
mod output;
mod report;
mod role;
mod role_causal_antichain;
mod role_causal_set;
mod value;

pub use atom::{AtomSet, TimedAtom};
pub use causal_set::CausalSet;
pub use domain::{CausalDomain, RoleCausalDomain};
pub use explanation::{RoleCause, RoleExplanation};
pub use output::CausalJsonlOutputHandler;
pub use report::{
    CausalCauseReport, CausalExplanationReport, CausalReport, CausalReportBatch, CausalReportValue,
    CausalResultReport, causality_report, report_batch, report_batch_json, report_batch_json_line,
};
pub use role::CausalRole;
pub use role::CausalRoles;
pub use role_causal_antichain::RoleCausalAntichain;
pub use role_causal_set::RoleCausalSet;
pub use value::CausalValue;
