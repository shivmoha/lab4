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
    let mut nlcommit = 0;

    for msg in plog.iter() {
        let pm = ProtocolMessage::from_string(&String::from_utf8_lossy(msg.payload()).as_ref().to_string());
        match pm.mtype {
            MessageType::CoordinatorCommit => {
                npcommit += 1;
            }
            MessageType::CoordinatorAbort => {
                npabort += 1
            }
            MessageType::ParticipantVoteCommit => {
                nlcommit += 1
            }
            _ => {}
        }
    }

    result &= (npcommit <= ncommit) && (nlcommit >= ncommit);
    result &= npabort <= nabort;
    debug!("{} ncommit={} nlcommit={} nabort={} npabort={}", participant,ncommit,nlcommit,nabort,npabort);
    assert!(ncommit <= nlcommit);
    assert!(npcommit <= ncommit); //npcommit = # coordinator commit in participant log  .. ncommit = # of commits
    assert!(nabort >= npabort);

    for v in ccommitted.iter() {
        let cpm = ProtocolMessage::from_string(&String::from_utf8_lossy(v.payload()).as_ref().to_string());
        let txid = cpm.txid;
        let mut foundlocaltxid = 0;
        if cpm.mtype == MessageType::CoordinatorCommit {
            for v3 in plog.iter() {
                let pm = ProtocolMessage::from_string(&String::from_utf8_lossy(v3.payload()).as_ref().to_string());
                if pm.mtype == MessageType::ParticipantVoteCommit {
                    if pm.txid == txid {
                        foundlocaltxid += 1;
                    }
                }
            }
            result &= foundlocaltxid == 1;
            assert!(foundlocaltxid == 1); // exactly one commit of txid per participant
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
