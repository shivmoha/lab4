//!
//! checker
//! Tools for checking output logs produced by the _T_wo _P_hase _C_ommit
//! project in run mode. Exports a single public function called check_last_run
//! that accepts a directory where client, participant, and coordinator log files
//! are found, and the number of clients, participants. Loads and analyses
//! log files to check a handful of correctness invariants.
//!
//! YOU SHOULD NOT NEED TO CHANGE CODE IN THIS FILE.
//!
extern crate clap;
extern crate ctrlc;
extern crate log;
extern crate stderrlog;

use std::borrow::Borrow;
use std::collections::HashMap;

use commitlog::message::{MessageBuf, MessageSet};

use comlog::CLog;
use message;
use message::MessageType;
use message::MessageType::ParticipantRequestRecovery;
use message::ProtocolMessage;

///
/// check_participant()
///
/// Given a participant name and HashMaps that represents the log files
/// for the participant and coordinator (already filtered for commit records),
/// check that the committed and aborted transactions are agreed upon by the two.
///
/// <params>
///     participant: name of participant (label)
///     ncommit: number of committed transactions from coordinator
///     nabort: number of aborted transactions from coordinator
///     ccommitted: map of committed transactions from coordinator
///     plog: map of participant operations
///
fn check_participant(
    participant: &String,
    ncommit: usize,
    nabort: usize,
    ccommitted: &MessageBuf,
    plog: &String,
) -> bool {
    let mut result = true;

    let plog = CLog::from_file(plog.clone());

    let mut npcommit = 0;
    let mut npabort = 0;

    for msg in plog.iter() {
        let pm = ProtocolMessage::from_string(&String::from_utf8_lossy(msg.payload()).as_ref().to_string());
        match pm.mtype {
            MessageType::CoordinatorCommit => {
                npcommit += 1;
            }
            MessageType::CoordinatorAbort => {
                npabort += 1
            }
            _ => {}
        }
    }

    result &= npcommit == ncommit;
    result &= npabort <= nabort;
    assert_eq!(ncommit, npcommit);
    assert!(nabort >= npabort);

    for v in ccommitted.iter() {
        let cpm = ProtocolMessage::from_string(&String::from_utf8_lossy(v.payload()).as_ref().to_string());
        let txid = cpm.txid;
        let mut foundtxid = 0;
        if cpm.mtype == MessageType::CoordinatorCommit {
            for v2 in plog.iter() {
                let pm = ProtocolMessage::from_string(&String::from_utf8_lossy(v2.payload()).as_ref().to_string());
                if pm.mtype == MessageType::CoordinatorCommit {
                    if pm.txid == txid {
                        foundtxid += 1;
                    }
                }
            }
            result &= foundtxid == 1;
            assert_eq!(foundtxid, 1); // exactly one commit of txid per participant
        }
    }
    println!("{} OK: C:{} == {}(C-global), A:{} <= {}(A-global)",
             participant.clone(),
             npcommit,
             ncommit,
             npabort,
             nabort);
    result
}

///
/// check_last_run()
///
/// accepts a directory where client, participant, and coordinator log files
/// are found, and the number of clients, participants. Loads and analyses
/// log files to check a handful of correctness invariants.
///
/// <params>
///     n_clients: number of clients
///     n_requests: number of requests per client
///     n_participants: number of participants
///     logpathbase: directory for client, participant, and coordinator logs
///
pub fn check_last_run(
    n_clients: i32,
    n_requests: i32,
    n_participants: i32,
    logpathbase: &String) {
    info!("Checking 2PC run:  {} requests * {} clients, {} participants",
          n_requests,
          n_clients,
          n_participants);

    let mut logs = HashMap::new();
    for pid in 0..n_participants {
        let pid_str = format!("participant_{}", pid);
        let plogpath = format!("{}//{}.commitlog", logpathbase, pid_str);
        logs.insert(pid_str, plogpath.clone());
    }
    let clogpath = format!("{}//{}", logpathbase, "coordinator.commitlog");
    let clog = CLog::from_file(clogpath);

    let mut ncommit = 0;
    let mut nabort = 0;

    for msg in clog.iter() {
        let pm = ProtocolMessage::from_string(&String::from_utf8_lossy(msg.payload()).as_ref().to_string());
        match pm.mtype {
            MessageType::CoordinatorCommit => {
                ncommit += 1
            }
            MessageType::CoordinatorAbort => {
                nabort += 1
            }
            _ => {}
        }
    }

    for (p, v) in logs.iter() {
        check_participant(p, ncommit, nabort, &clog, &v);
    }
}
