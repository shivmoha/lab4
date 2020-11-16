//! 
//! participant.rs
//! Implementation of 2PC participant
//! 
extern crate log;
extern crate rand;
extern crate stderrlog;

use std::alloc::dealloc;
use std::collections::HashMap;
use std::process::exit;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::atomic::AtomicI32;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::thread;
use std::thread::sleep;
use std::time::Duration;

use commitlog::message::MessageSet;
use serde_json::map::Entry;

use ::{message, oplog};
use comlog::CLog;
use message::MessageType;
use message::MessageType::{ParticipantRequestRecovery, ParticipantVoteAbort, ParticipantVoteCommit};
use message::ProtocolMessage;
use message::RequestStatus;
use oplog::OpLog;
use participant::rand::prelude::*;

use self::rand::random;

///
/// ParticipantState
/// enum for participant 2PC state machine
/// 
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticipantState {
    Quiescent,
}

///
/// Participant
/// structure for maintaining per-participant state 
/// and communication/synchronization objects to/from coordinator
/// 
#[derive(Debug)]
pub struct Participant {
    id: i32,
    state: ParticipantState,
    logOp: OpLog,
    logCommit: CLog,
    op_success_prob: f64,
    msg_success_prob: f64,
    sender: crossbeam_channel::Sender<ProtocolMessage>,
    receiver: crossbeam_channel::Receiver<ProtocolMessage>,
    running: Arc<AtomicBool>,
    successful: usize,
    failed: usize,
    unknown: usize,
    commitLog: bool,
    failure_prob: f64,
}

///
/// Participant
/// implementation of per-participant 2PC protocol
/// Required:
/// 1. new -- ctor
/// 2. pub fn report_status -- reports number of committed/aborted/unknown for each participant
/// 3. pub fn protocol() -- implements participant side protocol
///
impl Participant {
    ///
    /// new()
    /// 
    /// Return a new participant, ready to run the 2PC protocol
    /// with the coordinator. 
    /// 
    /// HINT: you may want to pass some channels or other communication 
    ///       objects that enable coordinator->participant and participant->coordinator
    ///       messaging to this ctor.
    /// HINT: you may want to pass some global flags that indicate whether
    ///       the protocol is still running to this constructor. There are other
    ///       ways to communicate this, of course. 
    /// 
    pub fn new(
        i: i32,
        is: String,
        send: crossbeam_channel::Sender<ProtocolMessage>,
        receive: crossbeam_channel::Receiver<ProtocolMessage>,
        logpath: String,
        r: Arc<AtomicBool>,
        f_success_prob_ops: f64,
        f_success_prob_msg: f64,
        commitLog: bool,
        participant_failure_prob: f64) -> Participant {
        let participantName = format!("{}{}", "participant_", i);
        let participantOpLogPath = format!("{}/{}.log", logpath.clone(), participantName);
        let participantCommitLogPath = format!("{}/{}.commitlog", logpath, participantName);

        Participant {
            id: i,
            logOp: OpLog::new(participantOpLogPath),
            logCommit: CLog::new(participantCommitLogPath),
            op_success_prob: f_success_prob_ops,
            msg_success_prob: f_success_prob_msg,
            state: ParticipantState::Quiescent,
            sender: send,
            receiver: receive,
            running: r,
            successful: 0,
            failed: 0,
            unknown: 0,
            commitLog,
            failure_prob: participant_failure_prob,
        }
    }

    ///
    /// send()
    /// Send a protocol message to the coordinator.
    /// This variant can be assumed to always succeed.
    /// You should make sure your solution works using this 
    /// variant before working with the send_unreliable variant.
    /// 
    /// HINT: you will need to implement something that does the 
    ///       actual sending.
    /// 
    pub fn send(&mut self, pm: ProtocolMessage) -> bool {
        let result: bool = true;
        self.sender.send(pm).unwrap();
        result
    }

    ///
    /// send()
    /// Send a protocol message to the coordinator, 
    /// with some probability of success thresholded by the 
    /// command line option success_probability [0.0..1.0].
    /// This variant can be assumed to always succeed
    /// 
    /// HINT: you will need to implement something that does the 
    ///       actual sending, but you can use the threshold 
    ///       logic in this implementation below. 
    /// 
    pub fn send_unreliable(&mut self, pm: ProtocolMessage) -> bool {
        let x: f64 = random();
        let result: bool;
        if x < self.msg_success_prob {
            result = self.send(pm);
        } else {
            debug!("Message Dropped, Start Recovery");
            result = false;
        }
        result
    }

    /// 
    /// perform_operation
    /// perform the operation specified in the 2PC proposal,
    /// with some probability of success/failure determined by the 
    /// command-line option success_probability. 
    /// 
    /// HINT: The code provided here is not complete--it provides some
    ///       tracing infrastructure and the probability logic. 
    ///       Your implementation need not preserve the method signature
    ///       (it's ok to add parameters or return something other than 
    ///       bool if it's more convenient for your design).
    /// 
    pub fn perform_operation(&mut self, request: &Option<ProtocolMessage>) -> bool {
        trace!("participant::perform_operation");
        let mut result: RequestStatus = RequestStatus::Unknown;
        let request_message = request.clone().expect("Error in performing operation");
        let x: f64 = random();
        if x > self.op_success_prob {
            self.appendToLog(ParticipantVoteAbort, request_message.clone().txid, request_message.clone().senderid, request_message.clone().opid);
            result = RequestStatus::Aborted;
        } else {
            self.appendToLog(ParticipantVoteCommit, request_message.clone().txid, request_message.clone().senderid, request_message.clone().opid);
            result = RequestStatus::Committed;
        };
        trace!("exit participant::perform_operation");
        result == RequestStatus::Committed
    }

    ///
    /// report_status()
    /// report the abort/commit/unknown status (aggregate) of all 
    /// transaction requests made by this coordinator before exiting. 
    /// 
    pub fn report_status(&mut self) {
        println!("participant_{}:\tC:{}\tA:{}\tU:{}", self.id, self.successful, self.failed, self.unknown);
    }

    ///
    /// wait_for_exit_signal(&mut self)
    /// wait until the running flag is set by the CTRL-C handler
    /// 
    pub fn wait_for_exit_signal(&mut self) {
        trace!("participant_{} waiting for exit signal", self.id);
        // TODO
        trace!("participant_{} exiting", self.id);
    }


    pub fn triggerRecoveryProtocol(&mut self) {
        //Map of transaction_id to count
        debug!(" Participant_{} : Initiated Recovery", self.id);
        let mut recoveryTransactions: HashMap<i32, i32> = HashMap::new();
        if self.commitLog == true {

            for msg in self.logCommit.readAllMessage().iter() {
                let cmsg = ProtocolMessage::from_string(&String::from_utf8_lossy(msg.payload()).as_ref().to_string());
                let existing = recoveryTransactions.get(&cmsg.txid);
                if existing.is_none() {
                    recoveryTransactions.insert(cmsg.txid, 1);
                } else {
                    recoveryTransactions.insert(cmsg.txid, existing.unwrap() + 1);
                }
            }

            let coordinatorLogs = CLog::from_file("./tmp/coordinator.commitlog".parse().unwrap());
            for (k, v) in recoveryTransactions.iter() {
                if *v == 1 {
                    for msg in coordinatorLogs.iter() {
                        let cmsg = ProtocolMessage::from_string(&String::from_utf8_lossy(msg.payload()).as_ref().to_string());
                        if cmsg.txid == *k {
                            debug!("Coordinator:: Sending recovery response to participant_{}", self.id);
                            self.appendToLog(cmsg.clone().mtype, cmsg.clone().txid,
                                             cmsg.clone().senderid, cmsg.clone().opid);
                        }
                    }
                }
            }

        } else {

            for (_, message) in self.logOp.arc().lock().unwrap().iter() {
                let existing = recoveryTransactions.get(&message.txid);
                if existing.is_none() {
                    recoveryTransactions.insert(message.txid, 1);
                } else {
                    recoveryTransactions.insert(message.txid, existing.unwrap() + 1);
                }
            }

            let coordinatorLogs = OpLog::from_file("./tmp/coordinator.log".parse().unwrap());
            for (k, v) in recoveryTransactions.iter() {
                if *v == 1 {
                    for (_, message) in coordinatorLogs.arc().lock().unwrap().iter() {
                        if message.txid == *k {
                            warn!("participant_{} : recovered for msh {:?}", self.id, message);
                            self.appendToLog(message.clone().mtype, message.clone().txid,
                                             message.clone().senderid, message.clone().opid);
                        }
                    }
                }
            }

        }
    }


    fn appendToLog(&mut self, t: message::MessageType, tid: i32, sender: String, op: i32) {
        if self.commitLog == true {
            self.logCommit.append(t, tid, sender, op);
        } else {
            self.logOp.append(t, tid, sender, op);
        }
    }

    ///
    /// protocol()
    /// Implements the participant side of the 2PC protocol
    /// HINT: if the simulation ends early, don't keep handling requests!
    /// HINT: wait for some kind of exit signal before returning from the protocol!
    /// 
    pub fn protocol(&mut self) {
        let mut coordinatorExit = false;
        let mut requestProcessed = 0;
        //Coming up, maybe from crash, try recovery
        self.triggerRecoveryProtocol();
        while coordinatorExit == false {
            // Receive client request
            let message = self.receiver.recv();
            if message.is_err() {
                continue;
            }
            requestProcessed += 1;
            let message = message.expect("Participant :: Error in receiving message from coordinator");
            debug!("Participant_{}  : Message received :: {:?}", self.id, message);
            match message.clone().mtype {
                MessageType::ClientRequest => {
                    debug!("Participant_{}: Operation Received", self.id);
                    let operationResult = self.perform_operation(&Some(message.clone()));
                    if operationResult == true {
                        self.send_unreliable(ProtocolMessage::generate(ParticipantVoteCommit, message.clone().txid, message.clone().senderid, message.clone().opid));
                        debug!("Participant_{}: Response Send Commit", self.id);
                        self.successful += 1;
                    } else {
                        self.send_unreliable(ProtocolMessage::generate(ParticipantVoteAbort, message.clone().txid, message.clone().senderid, message.clone().opid));
                        debug!("Participant_{}: Response Send Abort", self.id);
                        self.failed += 1;
                    }
                }
                MessageType::CoordinatorCommit => {
                    let x: f64 = random();
                    if x < self.failure_prob {
                        self.triggerRecoveryProtocol();
                    } else {
                        self.appendToLog(MessageType::CoordinatorCommit, message.clone().txid, message.clone().senderid, message.clone().opid);
                        debug!("Participant_{}: Received CoordinatorCommit", self.id);
                    }
                }
                MessageType::CoordinatorAbort => {
                    self.appendToLog(MessageType::CoordinatorAbort, message.clone().txid, message.clone().senderid, message.clone().opid);
                    debug!("Participant_{}: Received CoordinatorAbort", self.id);
                }
                MessageType::CoordinatorExit => { coordinatorExit = true; }
                _ => debug!("No match found")
            }
        } //End of while
        // self.wait_for_exit_signal();
        self.report_status();
        info!("Participant_{}::Shutting Down", self.id);
    }
}
