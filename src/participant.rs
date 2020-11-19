//! 
//! participant.rs
//! Implementation of 2PC participant
//! 
extern crate log;
extern crate rand;
extern crate stderrlog;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use commitlog::message::MessageSet;

use ::{message, oplog};
use comlog::CLog;
use message::MessageType;
use message::MessageType::{ParticipantVoteAbort, ParticipantVoteCommit};
use message::ProtocolMessage;
use message::RequestStatus;
use oplog::OpLog;

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
    logop: OpLog,
    logcommit: CLog,
    op_success_prob: f64,
    msg_success_prob: f64,
    sender: crossbeam_channel::Sender<ProtocolMessage>,
    receiver: crossbeam_channel::Receiver<ProtocolMessage>,
    running: Arc<AtomicBool>,
    successful: usize,
    failed: usize,
    unknown: usize,
    commit_log: bool,
    failure_prob: f64,
    logpath: String,
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
        commit_log: bool,
        participant_failure_prob: f64) -> Participant {
        let participant_name = format!("{}{}", "participant_", i);
        let participant_oplog_path = format!("{}/{}.log", logpath.clone(), participant_name);
        let participant_commitlog_path = format!("{}/{}.commitlog", logpath, participant_name);

        Participant {
            id: i,
            logop: OpLog::new(participant_oplog_path),
            logcommit: CLog::new(participant_commitlog_path),
            op_success_prob: f_success_prob_ops,
            msg_success_prob: f_success_prob_msg,
            state: ParticipantState::Quiescent,
            sender: send,
            receiver: receive,
            running: r,
            successful: 0,
            failed: 0,
            unknown: 0,
            commit_log: commit_log,
            failure_prob: participant_failure_prob,
            logpath: logpath,
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
            self.append_to_log(ParticipantVoteAbort, request_message.clone().txid, request_message.clone().senderid, request_message.clone().opid);
            result = RequestStatus::Aborted;
        } else {
            self.append_to_log(ParticipantVoteCommit, request_message.clone().txid, request_message.clone().senderid, request_message.clone().opid);
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


    pub fn trigger_recovery_protocol(&mut self) {
        //Map of transaction_id to count
        debug!(" Participant_{} : Initiated Recovery", self.id);
        let mut recovery_transactions: HashMap<i32, i32> = HashMap::new();
        if self.commit_log == true {
            for msg in self.logcommit.read_all_message().iter() {
                let cmsg = ProtocolMessage::from_string(&String::from_utf8_lossy(msg.payload()).as_ref().to_string());
                if cmsg.mtype == MessageType::ParticipantVoteCommit {
                    recovery_transactions.insert(cmsg.txid, recovery_transactions.get(&cmsg.txid).unwrap_or(&0) + 1);
                } else if cmsg.mtype == MessageType::CoordinatorCommit || cmsg.mtype == MessageType::CoordinatorAbort {
                    recovery_transactions.insert(cmsg.txid, recovery_transactions.get(&cmsg.txid).unwrap_or(&0) - 1);
                }
            }
            let commitLogPath = format!("{}/{}", self.logpath, "coordinator.commitlog");
            let coordinator_logs = CLog::from_file(commitLogPath);
            for (k, v) in recovery_transactions.iter() {
                if *v == 1 {
                    for msg in coordinator_logs.iter() {
                        let cmsg = ProtocolMessage::from_string(&String::from_utf8_lossy(msg.payload()).as_ref().to_string());
                        if cmsg.txid == *k {
                            self.append_to_log(cmsg.clone().mtype, cmsg.clone().txid,
                                               cmsg.clone().senderid, cmsg.clone().opid);
                        }
                    }
                }
            }
        } else {
            for (_, message) in self.logop.arc().lock().unwrap().iter() {
                if message.mtype == MessageType::ParticipantVoteCommit {
                    recovery_transactions.insert(message.txid, recovery_transactions.get(&message.txid).unwrap_or(&0) + 1);
                } else if message.mtype == MessageType::CoordinatorCommit || message.mtype == MessageType::CoordinatorAbort {
                    recovery_transactions.insert(message.txid, recovery_transactions.get(&message.txid).unwrap_or(&0) - 1);
                }
            }
            let opLogPath = format!("{}/{}", self.logpath, "coordinator.log");
            let coordinator_logs = OpLog::from_file(opLogPath);
            for (k, v) in recovery_transactions.iter() {
                if *v == 1 {
                    for (_, message) in coordinator_logs.arc().lock().unwrap().iter() {
                        if message.txid == *k {
                            warn!("participant_{} : recovered for msh {:?}", self.id, message);
                            self.append_to_log(message.clone().mtype, message.clone().txid,
                                               message.clone().senderid, message.clone().opid);
                        }
                    }
                }
            }
        }
    }


    fn append_to_log(&mut self, t: message::MessageType, tid: i32, sender: String, op: i32) {
        if self.commit_log == true {
            self.logcommit.append(t, tid, sender, op);
        } else {
            self.logop.append(t, tid, sender, op);
        }
    }

    ///
    /// protocol()
    /// Implements the participant side of the 2PC protocol
    /// HINT: if the simulation ends early, don't keep handling requests!
    /// HINT: wait for some kind of exit signal before returning from the protocol!
    /// 
    pub fn protocol(&mut self) {
        let mut coordinator_exit = false;
        let mut request_processed = 0;
        //Coming up, maybe from crash, try recovery
        self.trigger_recovery_protocol();
        while coordinator_exit == false {
            // Receive client request
            let message = self.receiver.recv();
            if message.is_err() {
                continue;
            }
            request_processed += 1;
            let message = message.expect("Participant :: Error in receiving message from coordinator");
            debug!("Participant_{}  : Message received :: {:?}", self.id, message);
            match message.clone().mtype {
                MessageType::ClientRequest => {
                    debug!("Participant_{}: Operation Received", self.id);
                    let operation_result = self.perform_operation(&Some(message.clone()));
                    if operation_result == true {
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
                        self.trigger_recovery_protocol();
                    } else {
                        self.append_to_log(MessageType::CoordinatorCommit, message.clone().txid, message.clone().senderid, message.clone().opid);
                        debug!("Participant_{}: Received CoordinatorCommit", self.id);
                    }
                }
                MessageType::CoordinatorAbort => {
                    self.append_to_log(MessageType::CoordinatorAbort, message.clone().txid, message.clone().senderid, message.clone().opid);
                    debug!("Participant_{}: Received CoordinatorAbort", self.id);
                }
                MessageType::CoordinatorExit => { coordinator_exit = true; }
                _ => debug!("No match found")
            }
        } //End of while
        self.report_status();
        info!("Participant_{}::Shutting Down", self.id);
    }
}
