// SPDX-License-Identifier: MIT

//! The payment-service binary: the Rust port of gluon's Scala
//! payment-service (ADR 0029). Generated from `tools/rust-service-template`:
//! the `example` table and `GET /examples/{id}` show the shape and go when
//! the first real route arrives.
pub mod clock;
pub mod config;
pub mod consumers;
pub mod db;
pub mod domain;
pub mod events;
pub mod ids;
pub mod lifecycle;
pub mod published;
pub mod server;
pub mod store;

/// The package version, as built.
#[must_use]
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
