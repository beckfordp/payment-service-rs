// SPDX-License-Identifier: MIT

//! The payment-service binary: the Rust port of gluon's Scala
//! payment-service (ADR 0029): payment methods, payments, authorizations
//! against the simulated gateway, captured or voided by the consumers, the
//! expiry sweep, and reconciliation flags.
pub mod config;
pub mod consumers;
pub mod db;
pub mod docs;
pub mod domain;
pub mod events;
pub mod lifecycle;
pub mod published;
pub mod routes;
pub mod server;
pub mod store;

/// The package version, as built.
#[must_use]
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
