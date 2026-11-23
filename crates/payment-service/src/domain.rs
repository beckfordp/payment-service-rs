// SPDX-License-Identifier: MIT

//! The domain, as the Scala service's (`domain/*`, `Card`, `Address`,
//! `PaymentGatewayClient`, `OperatorAuth`): values parsed at the edge, so the
//! rest of the service holds only valid ones.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

/// Java's `UUID.fromString` for the canonical form: 8-4-4-4-12 hex digits.
/// (Rust's parser also takes braced, URN and unhyphenated forms; Java doesn't.)
#[must_use]
pub fn parse_uuid(raw: &str) -> Option<Uuid> {
    let canonical = raw.len() == 36
        && raw
            .char_indices()
            .all(|(i, c)| matches!(i, 8 | 13 | 18 | 23) == (c == '-'));
    canonical.then(|| Uuid::parse_str(raw).ok()).flatten()
}

/// An order id: any text that isn't blank (order-service's are UUIDs; the
/// payment doesn't check).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderId(String);

impl OrderId {
    /// `raw` as it came, unless it is blank.
    ///
    /// # Errors
    /// "orderId is required" for a blank one.
    pub fn parse(raw: &str) -> Result<Self, String> {
        if raw.trim().is_empty() {
            Err("orderId is required".to_owned())
        } else {
            Ok(Self(raw.to_owned()))
        }
    }

    /// The text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A new payment's rules: an order id, then a positive amount (`PaymentRules`).
///
/// # Errors
/// The first rule broken, as its reason.
pub fn payment_rules(order_id: &str, amount_cents: i32) -> Result<OrderId, String> {
    let order = OrderId::parse(order_id)?;
    if amount_cents > 0 {
        Ok(order)
    } else {
        Err("amountCents must be positive".to_owned())
    }
}

/// A payment's lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaymentStatus {
    /// Recorded by `POST /payments`, nothing held.
    Pending,
    /// A live hold.
    Authorized,
    /// Money moved.
    Captured,
    /// Declined, at authorize or capture.
    Failed,
    /// The hold released.
    Voided,
}

impl PaymentStatus {
    /// As stored and answered.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Authorized => "authorized",
            Self::Captured => "captured",
            Self::Failed => "failed",
            Self::Voided => "voided",
        }
    }

    /// From its stored text.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        [
            Self::Pending,
            Self::Authorized,
            Self::Captured,
            Self::Failed,
            Self::Voided,
        ]
        .into_iter()
        .find(|status| status.as_str() == raw)
    }
}

/// A stored payment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Payment {
    /// Its id.
    pub id: Uuid,
    /// The order it is for, unique.
    pub order_id: String,
    /// In cents.
    pub amount_cents: i32,
    /// Its status.
    pub status: PaymentStatus,
    /// When it was recorded.
    pub created_at: OffsetDateTime,
    /// Its last status change.
    pub updated_at: OffsetDateTime,
    /// The method it was authorized against, if any.
    pub payment_method_id: Option<Uuid>,
    /// The gateway's hold reference, if authorized.
    pub auth_id: Option<String>,
    /// When the hold lapses, if authorized.
    pub expires_at: Option<OffsetDateTime>,
    /// Why it was declined, if it was.
    pub decline_reason: Option<String>,
}

/// Whether a hold expiring at `expires_at` is still live at `now`.
#[must_use]
pub fn is_live(expires_at: Option<OffsetDateTime>, now: OffsetDateTime) -> bool {
    expires_at.is_some_and(|at| at > now)
}

/// A card's brand, from its number's prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CardBrand {
    /// Starts with 4.
    Visa,
    /// 51–55 or 2221–2720.
    Mastercard,
    /// 34 or 37.
    Amex,
    /// Anything else.
    Other,
}

impl CardBrand {
    /// Prefix rules only: enough for the simulator, not a full BIN table.
    #[must_use]
    pub fn of(digits: &str) -> Self {
        let prefix = |n: usize| digits.get(..n).and_then(|p| p.parse::<u32>().ok());
        if digits.starts_with('4') {
            Self::Visa
        } else if prefix(2).is_some_and(|p| (51..=55).contains(&p))
            || prefix(4).is_some_and(|p| (2221..=2720).contains(&p))
        {
            Self::Mastercard
        } else if matches!(prefix(2), Some(34 | 37)) {
            Self::Amex
        } else {
            Self::Other
        }
    }

    /// As stored and answered.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Visa => "visa",
            Self::Mastercard => "mastercard",
            Self::Amex => "amex",
            Self::Other => "other",
        }
    }

    /// From its stored text.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        [Self::Visa, Self::Mastercard, Self::Amex, Self::Other]
            .into_iter()
            .find(|brand| brand.as_str() == raw)
    }
}

/// What survives a card's validation: the number itself is dropped here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Card {
    /// Its last four digits.
    pub last4: String,
    /// Its brand.
    pub brand: CardBrand,
    /// `MM/YY`, as given.
    pub expiry: String,
}

impl Card {
    /// The number (spaces allowed) and expiry, judged in the month `today`
    /// (year, month): 13–19 digits passing Luhn, then `MM/YY`, then not past.
    ///
    /// # Errors
    /// The first rule broken, as its reason.
    pub fn validate(number: &str, expiry: &str, today: (i32, u8)) -> Result<Self, String> {
        let digits = number.replace(' ', "");
        if !(13..=19).contains(&digits.len())
            || !digits.bytes().all(|b| b.is_ascii_digit())
            || !luhn(&digits)
        {
            return Err("Invalid card number".to_owned());
        }
        let expires =
            month_year(expiry).ok_or_else(|| "Invalid expiry, expected MM/YY".to_owned())?;
        if expires < today {
            return Err("Card has expired".to_owned());
        }
        Ok(Self {
            last4: digits[digits.len() - 4..].to_owned(),
            brand: CardBrand::of(&digits),
            expiry: expiry.to_owned(),
        })
    }

    /// The simulated tokenizer: `tok_<brand>_<last4>`, deterministic so the
    /// gateway's test-card outcomes can name it.
    #[must_use]
    pub fn token(&self) -> String {
        format!("tok_{}_{}", self.brand.as_str(), self.last4)
    }
}

/// `MM/YY` as (year, month), the year in 2000–2099.
fn month_year(expiry: &str) -> Option<(i32, u8)> {
    let (mm, yy) = expiry.split_once('/')?;
    if mm.len() != 2 || yy.len() != 2 || !(mm.bytes().chain(yy.bytes())).all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let month: u8 = mm.parse().ok()?;
    let year: i32 = yy.parse().ok()?;
    (1..=12).contains(&month).then_some((2000 + year, month))
}

fn luhn(digits: &str) -> bool {
    let sum: u32 = digits
        .bytes()
        .rev()
        .enumerate()
        .map(|(i, b)| {
            let d = u32::from(b - b'0');
            if i % 2 == 0 {
                d
            } else if d * 2 > 9 {
                d * 2 - 9
            } else {
                d * 2
            }
        })
        .sum();
    sum.is_multiple_of(10)
}

/// A billing address, stored with the method as JSON.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(description = "Billing address.")]
pub struct Address {
    /// Street address.
    pub line1: String,
    /// Optional second address line.
    pub line2: Option<String>,
    /// City or town.
    pub city: String,
    /// Postal code.
    pub postal_code: String,
    /// ISO 3166-1 alpha-2 country code.
    pub country: String,
}

impl Address {
    /// line1, city and postalCode not blank, country two capital letters.
    ///
    /// # Errors
    /// The first rule broken, as its reason.
    pub fn validate(&self) -> Result<(), String> {
        for (field, value) in [
            ("line1", &self.line1),
            ("city", &self.city),
            ("postalCode", &self.postal_code),
        ] {
            if value.trim().is_empty() {
                return Err(format!("{field} is required"));
            }
        }
        if self.country.len() == 2 && self.country.bytes().all(|b| b.is_ascii_uppercase()) {
            Ok(())
        } else {
            Err("country must be an ISO 3166-1 alpha-2 code".to_owned())
        }
    }
}

/// A stored payment method: the token, never the number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaymentMethod {
    /// Its id.
    pub id: Uuid,
    /// Whose it is.
    pub customer_id: String,
    /// `tok_<brand>_<last4>`.
    pub card_token: String,
    /// The card's last four digits.
    pub last4: String,
    /// The card's brand.
    pub brand: CardBrand,
    /// `MM/YY`.
    pub expiry: String,
    /// The billing address.
    pub billing_address: Address,
}

/// A new method's rules, in the Scala order: a customer, the card, the address.
///
/// # Errors
/// The first rule broken, as its reason.
pub fn method_rules(
    customer_id: &str,
    number: &str,
    expiry: &str,
    address: &Address,
    today: (i32, u8),
) -> Result<Card, String> {
    if customer_id.trim().is_empty() {
        return Err("customerId is required".to_owned());
    }
    let card = Card::validate(number, expiry, today)?;
    address.validate()?;
    Ok(card)
}

/// A gateway outcome for a card token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Approved.
    Approve,
    /// Declined, with the reason.
    Decline(String),
}

/// A placed hold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Authorization {
    /// `auth_<uuid>`.
    pub auth_id: String,
    /// When it lapses.
    pub expires_at: OffsetDateTime,
}

/// The simulated gateway (`payment-gateway.simulated`): outcomes by card
/// token, holds lasting `ttl`; a void always succeeds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimulatedGateway {
    outcomes: HashMap<String, Outcome>,
    capture_outcomes: HashMap<String, Outcome>,
    ttl: Duration,
}

impl Default for SimulatedGateway {
    /// application.conf's: `tok_visa_0002` declines authorize
    /// (`insufficient_funds`), `tok_mastercard_4444` declines capture
    /// (`capture_declined`), everything else approves; holds last seven days.
    fn default() -> Self {
        let decline = |reason: &str| Outcome::Decline(reason.to_owned());
        Self {
            outcomes: HashMap::from([("tok_visa_0002".to_owned(), decline("insufficient_funds"))]),
            capture_outcomes: HashMap::from([(
                "tok_mastercard_4444".to_owned(),
                decline("capture_declined"),
            )]),
            ttl: Duration::days(7),
        }
    }
}

impl SimulatedGateway {
    /// A hold on `card_token` placed at `now`, or the decline reason.
    ///
    /// # Errors
    /// The configured decline.
    pub fn authorize(
        &self,
        card_token: &str,
        now: OffsetDateTime,
    ) -> Result<Authorization, String> {
        match self.outcomes.get(card_token) {
            Some(Outcome::Decline(reason)) => Err(reason.clone()),
            Some(Outcome::Approve) | None => Ok(Authorization {
                auth_id: format!("auth_{}", Uuid::new_v4()),
                expires_at: now + self.ttl,
            }),
        }
    }

    /// Captures a hold on `card_token`.
    ///
    /// # Errors
    /// The configured decline.
    pub fn capture(&self, card_token: &str) -> Result<(), String> {
        match self.capture_outcomes.get(card_token) {
            Some(Outcome::Decline(reason)) => Err(reason.clone()),
            Some(Outcome::Approve) | None => Ok(()),
        }
    }
}

/// What a conditional transition that matched no authorized row means, by the
/// status found instead (`PaymentRace.classify`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Race {
    /// Already in the target status.
    Duplicate,
    /// A late event for a terminal state no money moved in.
    Benign,
    /// Lost to another outcome: flag it with this reason.
    Lost(&'static str),
    /// Anything else: an error.
    Unexpected,
}

/// Classifies `current` against the `target` the transition wanted.
#[must_use]
pub fn race(target: PaymentStatus, current: PaymentStatus) -> Race {
    use PaymentStatus::{Captured, Failed, Voided};
    if target == current {
        return Race::Duplicate;
    }
    match (target, current) {
        (Voided, Captured) => Race::Lost(VOID_AFTER_CAPTURE),
        (Captured, Voided) => Race::Lost(CAPTURE_AFTER_VOID),
        (Captured, Failed) => Race::Lost(CAPTURE_AFTER_FAILED),
        (Failed, Captured) => Race::Lost("capture_failure_after_capture"),
        (Failed, Voided) | (Voided, Failed) => Race::Benign,
        _ => Race::Unexpected,
    }
}

/// A cancel or failed dispatch after money moved.
pub const VOID_AFTER_CAPTURE: &str = "void_after_capture";
/// A dispatch after the hold was voided.
pub const CAPTURE_AFTER_VOID: &str = "capture_after_void";
/// A dispatch after the payment failed.
pub const CAPTURE_AFTER_FAILED: &str = "capture_after_failed";
/// A dispatch after the hold lapsed.
pub const DISPATCH_AFTER_EXPIRY: &str = "dispatch_after_authorization_expired";
/// A capture decline's reason on the payment and in `PaymentFailed`.
pub const CAPTURE_FAILED: &str = "capture_failed";
/// The sweep's reason on a voided payment.
pub const AUTHORIZATION_EXPIRED: &str = "authorization_expired";

/// A reconciliation flag's status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconciliationStatus {
    /// Awaiting an operator.
    Open,
    /// An operator resolved it.
    Resolved,
}

impl ReconciliationStatus {
    /// As stored and answered.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Resolved => "resolved",
        }
    }

    /// From its text, exactly.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        [Self::Open, Self::Resolved]
            .into_iter()
            .find(|status| status.as_str() == raw)
    }
}

/// A manual-reconciliation flag: one per order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reconciliation {
    /// The order.
    pub order_id: String,
    /// Why.
    pub reason: String,
    /// Open or resolved.
    pub status: ReconciliationStatus,
    /// When it was flagged.
    pub created_at: OffsetDateTime,
    /// Who resolved it.
    pub operator_id: Option<String>,
    /// Their note.
    pub note: Option<String>,
    /// When.
    pub resolved_at: Option<OffsetDateTime>,
}

/// What resolving a flag did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveOutcome {
    /// Resolved now.
    Resolved(Reconciliation),
    /// Already resolved: the first resolver's, unchanged.
    AlreadyResolved(Reconciliation),
    /// No flag for the order.
    NotFound,
}

/// The dev operator token (ADR 0024): `Bearer <token>` stands for the operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorAuth {
    token: Option<String>,
    operator_id: String,
}

impl OperatorAuth {
    /// From config: a blank token is none (every request refused).
    #[must_use]
    pub fn new(token: &str, operator_id: &str) -> Self {
        let token = token.trim();
        Self {
            token: (!token.is_empty()).then(|| token.to_owned()),
            operator_id: operator_id.trim().to_owned(),
        }
    }

    /// The operator, when `authorization` is `Bearer <token>` (the scheme in
    /// any case, the token trimmed; compared in constant time).
    #[must_use]
    pub fn operator(&self, authorization: Option<&str>) -> Option<&str> {
        const SCHEME: &str = "bearer ";
        let expected = self.token.as_deref()?;
        let raw = authorization?;
        if raw.len() <= SCHEME.len() || !raw.is_char_boundary(SCHEME.len()) {
            return None;
        }
        let (scheme, presented) = raw.split_at(SCHEME.len());
        (scheme.eq_ignore_ascii_case(SCHEME) && constant_eq(presented.trim(), expected))
            .then_some(self.operator_id.as_str())
    }
}

/// Equal bytes, in time independent of where they differ (`MessageDigest.isEqual`).
fn constant_eq(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .fold(0u8, |diff, (x, y)| diff | (x ^ y))
            == 0
}
