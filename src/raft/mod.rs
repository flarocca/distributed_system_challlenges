use std::collections::HashSet;

use chrono::{DateTime, Duration, Utc};
use rand::Rng;

pub struct Vote {
    pub term: usize,
    pub granted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Leader,
    Follower,
    Candidate,
}

#[derive(Debug, Clone)]
pub struct RaftState {
    node_id: String,
    election_timeout: std::time::Duration,
    election_deadline: DateTime<Utc>,
    log: Log,
    state: State,
    term: usize,
    voted_for: Option<String>,
    votes: HashSet<String>,
    majority: usize,
}

impl RaftState {
    pub fn new(node_id: String, majority: usize, election_timeout: std::time::Duration) -> Self {
        Self {
            node_id,
            election_timeout,
            election_deadline: Utc::now(),
            log: Log::new(),
            state: State::Follower,
            term: 0,
            voted_for: None,
            votes: HashSet::new(),
            majority,
        }
    }

    pub fn state(&self) -> State {
        self.state
    }

    pub fn term(&self) -> usize {
        self.term
    }

    pub fn last_log_index(&self) -> usize {
        self.log.size()
    }

    pub fn last_log_term(&self) -> usize {
        // Safe because we always initialize the log
        // with at least one entry
        self.log.last().unwrap().term
    }

    pub fn become_candidate(&mut self) -> anyhow::Result<bool> {
        if self.election_deadline < Utc::now() {
            if self.state != State::Leader {
                self.state = State::Candidate;
            }

            self.advance_term(self.term + 1)?;
            self.voted_for = Some(self.node_id.clone());
            self.votes.clear();
            self.votes.insert(self.node_id.clone());
            self.reset_election_timeout();

            eprintln!(
                "{} | {} | Became candidate in term {}",
                Utc::now().format("%H:%M:%S%.3f"),
                self.node_id,
                self.term
            );

            return Ok(true);
        }

        Ok(false)
    }

    pub fn collect_vote(
        &mut self,
        voter_id: String,
        term_requested: usize,
        term_voted: usize,
        vote_granted: bool,
    ) -> anyhow::Result<()> {
        eprintln!(
            "{} | {} | Collecting vote from {}: requested term {}, voted term {}, granted: {}",
            Utc::now().format("%H:%M:%S%.3f"),
            self.node_id,
            voter_id,
            term_requested,
            term_voted,
            vote_granted
        );
        self.maybe_step_down(term_voted)?;

        if self.state == State::Candidate
            && term_requested == self.term
            && term_requested == term_voted
            && vote_granted
        {
            self.votes.insert(voter_id);
            eprintln!(
                "{} | {} | Votes collected: {:#?}",
                Utc::now().format("%H:%M:%S%.3f"),
                self.node_id,
                self.votes
            );

            if self.votes.len() >= self.majority {
                eprintln!(
                    "{} | {} | Majority reached with {} votes. Becoming leader",
                    Utc::now().format("%H:%M:%S%.3f"),
                    self.node_id,
                    self.votes.len()
                );
                self.become_leader();
            }
        }

        Ok(())
    }

    pub fn emit_vote(
        &mut self,
        candidate_id: String,
        term: usize,
        last_log_term: usize,
        last_log_index: usize,
    ) -> anyhow::Result<Vote> {
        eprintln!(
            "{} | {} | Emitting vote for candidate {} for term {} with log term {} and index {}",
            Utc::now().format("%H:%M:%S%.3f"),
            self.node_id,
            candidate_id,
            term,
            last_log_term,
            last_log_index
        );
        self.maybe_step_down(term)?;
        let mut vote_granted = false;

        let current_last_log_index = self.log.size();
        let current_last_log_term = self.log.last().unwrap().term;

        if self.term > term {
            eprintln!(
                "{} | {} | Candidate term {} lower than {}. Not granting vote",
                Utc::now().format("%H:%M:%S%.3f"),
                self.node_id,
                term,
                self.term
            );
        } else if let Some(voted_for) = &self.voted_for {
            eprintln!(
                "{} | {} | Already voted for {}. Not granting vote",
                Utc::now().format("%H:%M:%S%.3f"),
                self.node_id,
                voted_for
            );
        } else if last_log_term < current_last_log_term {
            eprintln!(
                "{} | {} | Have log entries from term {}, which is newer than remote term {}. Not granting vote",
                Utc::now().format("%H:%M:%S%.3f"),
                self.node_id,
                current_last_log_term,
                last_log_term
            );
        } else if last_log_term == current_last_log_term && last_log_index < current_last_log_index
        {
            eprintln!(
                "{} | {} | Our logs are both at term {} but our log is {} and theirs is only {} long. Not granting vote",
                Utc::now().format("%H:%M:%S%.3f"),
                self.node_id,
                current_last_log_term,
                current_last_log_index,
                last_log_index
            );
        } else {
            eprintln!(
                "{} | {} | Granting vote to candidate {}",
                Utc::now().format("%H:%M:%S%.3f"),
                self.node_id,
                candidate_id
            );
            vote_granted = true;
            self.voted_for = Some(candidate_id);
            self.reset_election_timeout();
        }

        Ok(Vote {
            term: self.term,
            granted: vote_granted,
        })
    }

    fn become_follower(&mut self) {
        self.state = State::Follower;
        self.reset_election_timeout();

        eprintln!(
            "{} | {} | Became follower in term {}",
            Utc::now().format("%H:%M:%S%.3f"),
            self.node_id,
            self.term
        );
    }

    fn become_leader(&mut self) {
        if self.state != State::Candidate {
            eprintln!(
                "{} | {} | Cannot become leader from state {:?}",
                Utc::now().format("%H:%M:%S%.3f"),
                self.node_id,
                self.state
            );
            return;
        }

        self.state = State::Leader;

        eprintln!(
            "{} | {} | Became leader in term {}",
            Utc::now().format("%H:%M:%S%.3f"),
            self.node_id,
            self.term
        );
    }

    fn advance_term(&mut self, new_term: usize) -> anyhow::Result<()> {
        if new_term <= self.term {
            return Err(anyhow::anyhow!(
                "Term is monotonic increasing only. Cannot move backwards from {} to {}",
                new_term,
                self.term
            ));
        }

        self.term = new_term;
        self.voted_for = None;

        Ok(())
    }

    fn maybe_step_down(&mut self, remote_term: usize) -> anyhow::Result<()> {
        if self.term < remote_term {
            eprintln!(
                "{} | {} | Stepping down from term {} to {}",
                Utc::now().format("%H:%M:%S%.3f"),
                self.node_id,
                self.term,
                remote_term
            );
            self.advance_term(remote_term)?;
            self.become_follower();
        }

        Ok(())
    }

    fn reset_election_timeout(&mut self) {
        let random_millis = rand::rng().random_range(1..=200);

        self.election_deadline =
            Utc::now() + self.election_timeout + Duration::milliseconds(random_millis);
    }
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub term: usize,
    pub command: String,
}

#[derive(Debug, Clone)]
struct Log {
    entries: Vec<LogEntry>,
}

impl Log {
    pub fn new() -> Self {
        let entries = [LogEntry {
            term: 0,
            command: String::from("Initial entry"),
        }];
        Self {
            entries: entries.to_vec(),
        }
    }

    pub fn append(&mut self, term: usize, command: String) {
        self.entries.push(LogEntry { term, command });
    }

    pub fn last(&self) -> Option<LogEntry> {
        self.entries.last().cloned()
    }

    pub fn size(&self) -> usize {
        self.entries.len()
    }
}
