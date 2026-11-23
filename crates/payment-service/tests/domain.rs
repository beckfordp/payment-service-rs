// SPDX-License-Identifier: MIT

//! The domain's rules, as the Scala service's (`Card`, `Address`,
//! `PaymentRules`, `PaymentRace`, the simulated gateway, `OperatorAuth`).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use payment_service::domain::{
    Address, CAPTURE_AFTER_FAILED, CAPTURE_AFTER_VOID, Card, CardBrand, OperatorAuth,
    PaymentStatus, Race, ReconciliationStatus, SimulatedGateway, VOID_AFTER_CAPTURE, is_live,
    method_rules, parse_uuid, payment_rules, race,
};
use time::macros::datetime;
use time::{Duration, OffsetDateTime};

const NOV_2026: (i32, u8) = (2026, 11);

fn address() -> Address {
    Address {
        line1: "1 High Street".to_owned(),
        line2: None,
        city: "London".to_owned(),
        postal_code: "SW1A 1AA".to_owned(),
        country: "GB".to_owned(),
    }
}

#[test]
fn a_uuid_is_javas_canonical_form_only() {
    assert!(parse_uuid("00000000-0000-4000-8000-000000000001").is_some());
    assert!(parse_uuid("00000000000040008000000000000001").is_none());
    assert!(parse_uuid("{00000000-0000-4000-8000-000000000001}").is_none());
    assert!(parse_uuid("nope").is_none());
}

#[test]
fn a_card_is_judged_by_number_then_expiry() {
    let card = Card::validate("4242 4242 4242 4242", "12/99", NOV_2026).unwrap();
    assert_eq!(
        (card.last4.as_str(), card.brand, card.expiry.as_str()),
        ("4242", CardBrand::Visa, "12/99")
    );
    assert_eq!(card.token(), "tok_visa_4242");
    for number in [
        "4242",
        "4242 4242 4242 4241",
        "4242a42424242424",
        "42424242424242424242",
    ] {
        assert_eq!(
            Card::validate(number, "12/99", NOV_2026).unwrap_err(),
            "Invalid card number",
            "{number}"
        );
    }
    for expiry in ["13/30", "00/30", "1/30", "12-30", "12/3x", "12/300"] {
        assert_eq!(
            Card::validate("4242424242424242", expiry, NOV_2026).unwrap_err(),
            "Invalid expiry, expected MM/YY",
            "{expiry}"
        );
    }
    assert_eq!(
        Card::validate("4242424242424242", "10/26", NOV_2026).unwrap_err(),
        "Card has expired"
    );
    assert!(Card::validate("4242424242424242", "11/26", NOV_2026).is_ok());
}

#[test]
fn the_brand_comes_from_the_prefix() {
    for (digits, brand) in [
        ("4000000000000002", CardBrand::Visa),
        ("5555555555554444", CardBrand::Mastercard),
        ("2221000000000009", CardBrand::Mastercard),
        ("378282246310005", CardBrand::Amex),
        ("6011111111111117", CardBrand::Other),
    ] {
        assert_eq!(CardBrand::of(digits), brand, "{digits}");
        assert_eq!(CardBrand::parse(brand.as_str()), Some(brand));
    }
    assert_eq!(CardBrand::parse("diners"), None);
}

#[test]
fn a_method_is_judged_customer_card_then_address() {
    let blank = |field: &str| {
        let mut changed = address();
        match field {
            "line1" => changed.line1 = " ".to_owned(),
            "city" => changed.city = String::new(),
            _ => changed.postal_code = String::new(),
        }
        changed
    };
    assert_eq!(
        method_rules(" ", "nope", "x", &blank("city"), NOV_2026).unwrap_err(),
        "customerId is required"
    );
    assert_eq!(
        method_rules("c", "nope", "x", &blank("city"), NOV_2026).unwrap_err(),
        "Invalid card number"
    );
    for (field, reason) in [
        ("line1", "line1 is required"),
        ("city", "city is required"),
        ("postalCode", "postalCode is required"),
    ] {
        assert_eq!(
            method_rules("c", "4242424242424242", "12/99", &blank(field), NOV_2026).unwrap_err(),
            reason
        );
    }
    for country in ["gb", "GBR", "G"] {
        let mut changed = address();
        changed.country = country.to_owned();
        assert_eq!(
            changed.validate().unwrap_err(),
            "country must be an ISO 3166-1 alpha-2 code"
        );
    }
    assert!(method_rules("c", "4242424242424242", "12/99", &address(), NOV_2026).is_ok());
}

#[test]
fn a_payment_needs_an_order_then_a_positive_amount() {
    assert_eq!(payment_rules(" ", 0).unwrap_err(), "orderId is required");
    assert_eq!(
        payment_rules("o", 0).unwrap_err(),
        "amountCents must be positive"
    );
    assert_eq!(payment_rules("order-1", 1).unwrap().as_str(), "order-1");
}

#[test]
fn statuses_read_back_as_written() {
    for status in [
        PaymentStatus::Pending,
        PaymentStatus::Authorized,
        PaymentStatus::Captured,
        PaymentStatus::Failed,
        PaymentStatus::Voided,
    ] {
        assert_eq!(PaymentStatus::parse(status.as_str()), Some(status));
    }
    assert_eq!(PaymentStatus::parse("settled"), None);
    assert_eq!(
        ReconciliationStatus::parse("resolved"),
        Some(ReconciliationStatus::Resolved)
    );
    assert_eq!(ReconciliationStatus::parse("Open"), None);
}

#[test]
fn a_hold_is_live_until_it_expires() {
    let now = datetime!(2026-11-23 12:00 UTC);
    assert!(is_live(Some(now + Duration::seconds(1)), now));
    assert!(!is_live(Some(now), now));
    assert!(!is_live(None, now));
}

#[test]
fn a_lost_transition_is_classified_as_scala_does() {
    use PaymentStatus::{Authorized, Captured, Failed, Pending, Voided};
    assert_eq!(race(Voided, Voided), Race::Duplicate);
    assert_eq!(race(Voided, Captured), Race::Lost(VOID_AFTER_CAPTURE));
    assert_eq!(race(Captured, Voided), Race::Lost(CAPTURE_AFTER_VOID));
    assert_eq!(race(Captured, Failed), Race::Lost(CAPTURE_AFTER_FAILED));
    assert_eq!(
        race(Failed, Captured),
        Race::Lost("capture_failure_after_capture")
    );
    assert_eq!(race(Failed, Voided), Race::Benign);
    assert_eq!(race(Voided, Failed), Race::Benign);
    assert_eq!(race(Captured, Pending), Race::Unexpected);
    assert_eq!(race(Voided, Authorized), Race::Unexpected);
}

#[test]
fn the_simulated_gateway_declines_its_test_cards() {
    let gateway = SimulatedGateway::default();
    let now = OffsetDateTime::now_utc();
    let hold = gateway.authorize("tok_visa_4242", now).unwrap();
    assert!(hold.auth_id.starts_with("auth_"));
    assert_eq!(hold.expires_at, now + Duration::days(7));
    assert_eq!(
        gateway.authorize("tok_visa_0002", now).unwrap_err(),
        "insufficient_funds"
    );
    assert!(gateway.capture("tok_visa_4242").is_ok());
    assert_eq!(
        gateway.capture("tok_mastercard_4444").unwrap_err(),
        "capture_declined"
    );
}

#[test]
fn the_operator_token_is_a_bearer_token() {
    let auth = OperatorAuth::new(" t0k ", " ops ");
    assert_eq!(auth.operator(Some("Bearer t0k")), Some("ops"));
    assert_eq!(auth.operator(Some("bearer  t0k ")), Some("ops"));
    assert_eq!(auth.operator(Some("Bearer t0x")), None);
    assert_eq!(auth.operator(Some("Basic t0k")), None);
    assert_eq!(auth.operator(Some("Bearer ")), None);
    assert_eq!(auth.operator(Some("Beé")), None);
    assert_eq!(auth.operator(None), None);
    assert_eq!(
        OperatorAuth::new(" ", "ops").operator(Some("Bearer ")),
        None
    );
}
