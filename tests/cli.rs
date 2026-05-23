use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn help_lists_usage_and_query() {
    Command::cargo_bin("rstn")
        .unwrap()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Usage:"))
        .stdout(predicate::str::contains("[QUERY]"));
}

#[test]
fn version_matches_cargo_package() {
    Command::cargo_bin("rstn")
        .unwrap()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains(env!("CARGO_PKG_VERSION")));
}

#[test]
fn requires_query_without_selector() {
    Command::cargo_bin("rstn")
        .unwrap()
        .assert()
        .failure()
        .code(1)
        .stderr(predicate::str::contains("QUERY"));
}

#[test]
fn label_selector_without_query_reaches_flag_validation() {
    Command::cargo_bin("rstn")
        .unwrap()
        .args(["-l", "app=foo", "--tail=-1"])
        .assert()
        .failure()
        .code(1)
        .stderr(predicate::str::contains("--tail must be >= 0"));
}

#[test]
fn field_selector_without_query_reaches_flag_validation() {
    Command::cargo_bin("rstn")
        .unwrap()
        .args(["--field-selector", "metadata.name=foo", "--tail=-1"])
        .assert()
        .failure()
        .code(1)
        .stderr(predicate::str::contains("--tail must be >= 0"));
}

#[test]
fn validate_rejects_negative_tail() {
    Command::cargo_bin("rstn")
        .unwrap()
        .args(["--tail=-1", "x"])
        .assert()
        .failure()
        .code(1)
        .stderr(predicate::str::contains("--tail must be >= 0"));
}

#[test]
fn namespace_and_all_namespaces_conflict() {
    Command::cargo_bin("rstn")
        .unwrap()
        .args(["-n", "ns", "-A", "x"])
        .assert()
        .failure()
        .code(2)
        .stderr(predicate::str::contains("cannot be used with"));
}

#[test]
fn validate_rejects_invalid_since() {
    Command::cargo_bin("rstn")
        .unwrap()
        .args(["--since", "not-a-duration", "x"])
        .assert()
        .failure()
        .code(1)
        .stderr(predicate::str::contains("invalid --since"));
}
