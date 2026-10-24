//! The outcome rule: how one call merges the results of its attempts.
//!
//! Only `committed` replaces `unknown`. A later `not_committed` cannot clear an
//! earlier unclear attempt, because the engine can reject a retry of a key whose
//! first attempt is already durable. An attempt that sent nothing, because connect failed
//! or under 2 s were left, counts as `not_committed`. The call still retries until the
//! deadline.

pub use zerodb_tape::Outcome;

/// Parses the wire form used in response bodies.
pub fn parse(text: &str) -> Option<Outcome> {
    [Outcome::Committed, Outcome::NotCommitted, Outcome::Unknown]
        .into_iter()
        .find(|outcome| name(*outcome) == text)
}

/// Returns the wire form used in response bodies.
pub fn name(outcome: Outcome) -> &'static str {
    match outcome {
        Outcome::Committed => "committed",
        Outcome::NotCommitted => "not_committed",
        Outcome::Unknown => "unknown",
    }
}

/// The result of one attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Attempt {
    /// The server answered with an outcome.
    Answered(Outcome),
    /// Reset, timeout, or a short or malformed response, after the request was sent.
    TransportError,
    /// Nothing was sent: connect failed, or under 2 s were left to connect or send.
    NotSent,
}

/// The merged state of one call across its attempts.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Merge {
    state: Option<Outcome>,
}

impl Merge {
    /// Folds in one attempt. Returns the outcome to report now, or `None` to retry
    /// with the same key until the deadline.
    pub fn record(&mut self, attempt: Attempt) -> Option<Outcome> {
        let next = match (self.state, attempt) {
            (_, Attempt::Answered(Outcome::Committed)) => Outcome::Committed,
            (
                Some(Outcome::Unknown),
                Attempt::Answered(Outcome::NotCommitted) | Attempt::NotSent,
            ) => Outcome::Unknown,
            (_, Attempt::Answered(Outcome::NotCommitted) | Attempt::NotSent) => {
                Outcome::NotCommitted
            }
            (_, Attempt::Answered(Outcome::Unknown) | Attempt::TransportError) => Outcome::Unknown,
        };
        self.state = Some(next);
        match (next, attempt) {
            (Outcome::Unknown, _) | (_, Attempt::NotSent) => None,
            (done, _) => Some(done),
        }
    }

    /// The outcome to report when the deadline passes. No attempt at all is `Unknown`.
    pub fn finish(&self) -> Outcome {
        self.state.unwrap_or(Outcome::Unknown)
    }
}

#[cfg(test)]
mod tests {
    use super::Outcome::{Committed as C, NotCommitted as N, Unknown as U};
    use super::*;

    const T: Attempt = Attempt::TransportError;
    const S: Attempt = Attempt::NotSent;
    const fn a(outcome: Outcome) -> Attempt {
        Attempt::Answered(outcome)
    }

    #[test]
    fn merge_follows_the_outcome_rule() {
        // (case, attempts, what the last attempt reports, result at the deadline)
        let cases: [(&str, &[Attempt], Option<Outcome>, Outcome); 13] = [
            ("first committed", &[a(C)], Some(C), C),
            ("committed after unknown", &[a(U), a(C)], Some(C), C),
            ("committed after transport", &[T, a(U), a(C)], Some(C), C),
            ("first not_committed", &[a(N)], Some(N), N),
            ("not_committed after transport", &[T, a(N)], None, U),
            ("not_committed after unknown", &[a(U), a(N)], None, U),
            ("not_committed twice after unknown", &[a(U), a(N), a(N)], None, U),
            ("first unknown", &[a(U)], None, U),
            ("first transport", &[T], None, U),
            ("only connect failures", &[S, S], None, N),
            ("connect failure after unknown", &[a(U), S], None, U),
            ("transport after connect failure", &[S, T], None, U),
            ("transport after unknown", &[a(U), T], None, U),
        ];
        for (case, attempts, step, finish) in cases {
            let mut merge = Merge::default();
            let steps: Vec<_> = attempts.iter().map(|attempt| merge.record(*attempt)).collect();
            assert_eq!(steps.last(), Some(&step), "{case}: step");
            assert_eq!(merge.finish(), finish, "{case}: deadline result");
        }
        assert_eq!(Merge::default().finish(), U, "no attempt before the deadline");
        for outcome in [C, N, U] {
            assert_eq!(parse(name(outcome)), Some(outcome), "{outcome:?} wire form");
        }
    }
}
