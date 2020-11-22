//! 
//! coordinator.rs
//! Implementation of 2PC coordinator
//! 
extern crate crossbeam_channel;
extern crate log;
extern crate rand;
extern crate stderrlog;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use comlog::CLog;
use message;
use message::MessageType;
use message::MessageType::{ClientResultAbort, ClientResultCommit, CoordinatorAbort, CoordinatorCommit, CoordinatorExit};
use message::ProtocolMessage;
use message::RequestStatus;
use oplog::OpLog;

use self::rand::random;

/// CoordinatorState
/// States for 2PC state machine
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoordinatorState {
    Quiescent,
}

/// Coordinator
/// struct maintaining state for coordinator
#[derive(Debug)]
pub struct Coordinator {
    state: CoordinatorState,
    log_op: OpLog,
    log_commit: CLog,
    op_success_prob: f64,
    participants_channels: Vec<(crossbeam_channel::Sender<ProtocolMessage>, crossbeam_channel::Receiver<ProtocolMessage>)>,
    clients_channels: Vec<(crossbeam_channel::Sender<ProtocolMessage>, crossbeam_channel::Receiver<ProtocolMessage>)>,
    running: Arc<AtomicBool>,
    successful: usize,
    failed: usize,
    unknown: usize,
    commit_log: bool,
    total_requests: i32,
}

///
/// Coordinator
/// implementation of coordinator functionality
/// Required:
/// 1. new -- ctor
/// 2. protocol -- implementation of coordinator side of protocol
/// 3. report_status -- report of aggregate commit/abort/unknown stats on exit.
/// 4. participant_join -- what to do when a participant joins
/// 5. client_join -- what to do when a client joins
/// 
impl Coordinator {
    ///
    /// new()
    /// Initialize a new coordinator
    /// 
    /// <params>
    ///     logpath: directory for log files --> create a new log there. 
    ///     r: atomic bool --> still running?
    ///     success_prob --> probability operations/sends succeed
    ///
    pub fn new(logpath: String, r: Arc<AtomicBool>, success_prob: f64, commitLog: bool, totalRequests: i32) -> Coordinator {
        let opLogPath = format!("{}/{}", logpath, "coordinator.log");
        let commitLogPath = format!("{}/{}", logpath, "coordinator.commitlog");
        Coordinator {
            state: CoordinatorState::Quiescent,
            log_op: OpLog::new(opLogPath),
            log_commit: CLog::new(commitLogPath),
            op_success_prob: success_prob,
            participants_channels: vec![],
            clients_channels: vec![],
            running: r,
            successful: 0,
            failed: 0,
            unknown: 0,
            commit_log: commitLog,
            total_requests: totalRequests,
        }
    }

    /// 
    /// participant_join()
    /// handle the addition of a new participant
    /// HINT: keep track of any channels involved!
    /// HINT: you'll probably need to change this routine's 
    ///       signature to return something!
    ///       (e.g. channel(s) to be used)
    /// 
    pub fn participant_join(&mut self, name: &String) -> (crossbeam_channel::Sender<ProtocolMessage>, crossbeam_channel::Receiver<ProtocolMessage>) {
        assert_eq!(self.state, CoordinatorState::Quiescent);
        let (coordinator_send, participant_receive) = crossbeam_channel::bounded(0);
        let (participant_send, coordinator_receive) = crossbeam_channel::bounded(0);
        self.participants_channels.push((coordinator_send, coordinator_receive));
        return (participant_send, participant_receive);
    }

    /// 
    /// client_join()
    /// handle the addition of a new client
    /// HINTS: keep track of any channels involved!
    /// HINT: you'll probably need to change this routine's 
    ///       signature to return something!
    ///       (e.g. channel(s) to be used)
    /// 
    pub fn client_join(&mut self, name: &String) -> (crossbeam_channel::Sender<ProtocolMessage>, crossbeam_channel::Receiver<ProtocolMessage>) {
        assert_eq!(self.state, CoordinatorState::Quiescent);
        let (coordinator_send, client_receive) = crossbeam_channel::bounded(0);
        let (client_send, coordinator_receive) = crossbeam_channel::bounded(0);
        self.clients_channels.push((coordinator_send, coordinator_receive));
        return (client_send, client_receive);
    }

    /// 
    /// send()
    /// send a message, maybe drop it
    /// HINT: you'll need to do something to implement 
    ///       the actual sending!
    /// 
    pub fn send(&mut self, sender: &crossbeam_channel::Sender<ProtocolMessage>, pm: ProtocolMessage) -> bool {
        sender.send(pm);
        return true;
    }

    /// 
    /// recv_request()
    /// receive a message from a client
    /// to start off the protocol.
    /// 
    pub fn recv_request(&mut self) -> (Option<ProtocolMessage>, usize) {
        let mut result = Option::None;
        assert!(self.state == CoordinatorState::Quiescent);
        trace!("coordinator::recv_request...");
        let client_channels = self.clients_channels.clone();
        for clientChannel in client_channels {
            let msg = clientChannel.1.try_recv();
            if msg.is_ok() {
                let client_id = msg.clone().unwrap().senderid.split("_").collect::<Vec<&str>>()[1].to_string();
                debug!("Coordinator:: Received request from client_{}", client_id);
                result = Option::from(msg.unwrap());
                return (result, client_id.parse::<usize>().unwrap());
            }
        }
        trace!("leaving coordinator::recv_request");
        return (result, 0);
    }

    ///
    /// report_status()
    /// report the abort/commit/unknown status (aggregate) of all
    /// transaction requests made by this coordinator before exiting.
    ///
    pub fn report_status(&mut self) {
        println!("coordinator:\tC:{}\tA:{}\tU:{}", self.successful, self.failed, self.unknown);
    }

    fn append_to_log(&mut self, t: message::MessageType, tid: i32, sender: String, op: i32) {
        if self.commit_log == true {
            self.log_commit.append(t, tid, sender, op);
        } else {
            self.log_op.append(t, tid, sender, op);
        }
    }

    ///
    /// protocol()
    /// Implements the coordinator side of the 2PC protocol
    /// HINT: if the simulation ends early, don't keep handling requests!
    /// HINT: wait for some kind of exit signal before returning from the protocol!
    ///
    pub fn protocol(&mut self) {
        let mut request_processed = 0;
        while self.running.load(Ordering::SeqCst) && request_processed < self.total_requests {
            // Receive request from client
            let client_request_and_id = self.recv_request();
            let client_request = client_request_and_id.0;
            let client_id = client_request_and_id.1;
            if client_request.is_none() {
                continue;
            }
            let client_request = client_request.expect("Error in receiving client request");
            let mut committed = 0;
            let mut aborted = 0;
            let mut unknown = 0;
            let mut i = 0;
            let participants_channels = self.participants_channels.clone();
            let participants_channelsR = self.participants_channels.clone();

            // PHASE: 1 Step 1 Send client request to participants
            for participant_channel in participants_channels {
                debug!("Coordinator:: Sending Request to Participant {}", i);
                let client_request_participant = client_request.clone();
                self.send(&participant_channel.0, client_request_participant);
                i = i + 1;
            }
            // PHASE: 1 Step 2 Receive responses from all participants
            i = 0;
            for participantChannel in participants_channelsR {
                let msg_result = participantChannel.1.recv_timeout(Duration::from_millis(20));
                // if the response is received before timeout, process the response
                if msg_result.is_ok() {
                    let msg = msg_result.unwrap();
                    match msg.mtype {
                        MessageType::ParticipantVoteCommit => {
                            debug!("Coordinator:: Participant_{} responded with commit", i);
                            committed += 1
                        }
                        MessageType::ParticipantVoteAbort => {
                            debug!("Coordinator:: Participant_{} responded with abort", i);
                            aborted += 1
                        }
                        _ => {}
                    }
                } else {
                    // if the response is not received before timeout, mark the status as unknown
                    debug!("Coordinator:: Participant_{} time-out, will kill the threads and trigger recovery", i);
                    unknown += 1;
                }
                i = i + 1;
            }
            // PHASE: 2 Someone voted abort or didn't respond,  send abort to all participants
            if aborted > 0 || unknown > 0 {
                self.append_to_log(CoordinatorAbort, client_request.clone().txid, client_request.clone().senderid, client_request.clone().opid);
                debug!("Someone voted abort or is unknown,  send abort to all participants");
                let participants_channels = self.participants_channels.clone();
                for participant_channel in participants_channels {
                    participant_channel.0.send(ProtocolMessage::generate(CoordinatorAbort, client_request.clone().txid,
                                                                         client_request.clone().senderid, client_request.clone().opid));
                    debug!("Coordinator:: Sending Abort to Participant");
                }
                debug!("Coordinator:: Sending Abort to Client : START");
                self.clients_channels[client_id].0.send(ProtocolMessage::generate(ClientResultAbort, client_request.clone().txid,
                                                                                  client_request.clone().senderid, client_request.clone().opid));
                debug!("Coordinator:: Sending Abort to Client : DONE");
                self.failed += 1
            } else {
                self.append_to_log(CoordinatorCommit, client_request.clone().txid, client_request.clone().senderid, client_request.clone().opid);
                debug!("All voted commit for tid: {} , send commit to all participants", client_request.clone().txid);
                let participants_channels = self.participants_channels.clone();
                let mut i = 0;
                for participant_channel in participants_channels {
                    participant_channel.0.send(ProtocolMessage::generate(CoordinatorCommit, client_request.clone().txid,
                                                                         client_request.clone().senderid, client_request.clone().opid));
                    debug!("Coordinator:: Sending Commit to Participant {}", i);
                    i = i + 1;
                }
                debug!("Coordinator:: Sending Commit to client {:?}", self.clients_channels[0].0.capacity());
                self.clients_channels[client_id].0.send(ProtocolMessage::generate(ClientResultCommit, client_request.clone().txid,
                                                                                  client_request.clone().senderid, client_request.clone().opid));
                debug!("Coordinator:: Sending Commit to client done");
                self.successful += 1;
            }
            request_processed += 1;
            warn!("* * * * * * * * Rquest Processed: {}", request_processed);
        }//end of while
        info!("Coordinator::All request processed");
        // Tell all participants and clients to shutdown
        let participants_channels = self.participants_channels.clone();
        for participant_channel in participants_channels {
            participant_channel.0.send(ProtocolMessage::generate(CoordinatorExit, 0,
                                                                 format!("{}", "Coordinator"), 0));
        }
        let client_channels = self.clients_channels.clone();
        for client_channel in client_channels {
            client_channel.0.try_send(ProtocolMessage::generate(CoordinatorExit, 0,
                                                                format!("{}", "Coordinator"), 0));
        }
        self.report_status();
        info!("Coordinator::Shutting Down");
    }
}
