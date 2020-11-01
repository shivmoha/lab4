//! 
//! coordinator.rs
//! Implementation of 2PC coordinator
//! 
extern crate crossbeam_channel;
extern crate log;
extern crate rand;
extern crate stderrlog;

use std::alloc::dealloc;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::atomic::AtomicI32;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::mpsc::channel;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use coordinator::rand::prelude::*;
use message;
use message::MessageType;
use message::MessageType::{ClientRequest, CoordinatorAbort, CoordinatorCommit};
use message::ProtocolMessage;
use message::RequestStatus;
use oplog;

use self::rand::random;

/// CoordinatorState
/// States for 2PC state machine
/// 
/// TODO: add and/or delete!
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoordinatorState {
    Quiescent,
    // TODO...
}

/// Coordinator
/// struct maintaining state for coordinator
#[derive(Debug)]
pub struct Coordinator {
    state: CoordinatorState,
    log: oplog::OpLog,
    op_success_prob: f64,
    participantsChannels: Vec<(crossbeam_channel::Sender<ProtocolMessage>, crossbeam_channel::Receiver<ProtocolMessage>)>,
    clientsChannels: Vec<(crossbeam_channel::Sender<ProtocolMessage>, crossbeam_channel::Receiver<ProtocolMessage>)>,
    running: Arc<AtomicBool>,

    // TODO: ...
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
    pub fn new(
        logpath: String,
        r: Arc<AtomicBool>,
        success_prob: f64) -> Coordinator {
        Coordinator {
            state: CoordinatorState::Quiescent,
            log: oplog::OpLog::new(logpath),
            op_success_prob: success_prob,
            participantsChannels: vec![],
            clientsChannels: vec![],
            running: r,
            // TODO...
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
        assert!(self.state == CoordinatorState::Quiescent);

        let (coordinatorSend, participantReceive) = crossbeam_channel::bounded(0);
        let (participantSend, coordinatorReceive) = crossbeam_channel::bounded(0);

        self.participantsChannels.push((coordinatorSend, coordinatorReceive));
        // TODO
        return (participantSend, participantReceive);
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
        assert!(self.state == CoordinatorState::Quiescent);

        // TODO
        let (coordinatorSend, clientReceive) = crossbeam_channel::bounded(0);
        let (clientSend, coordinatorReceive) = crossbeam_channel::bounded(0);

        self.clientsChannels.push((coordinatorSend, coordinatorReceive));
        return (clientSend, clientReceive);
    }

    /// 
    /// send()
    /// send a message, maybe drop it
    /// HINT: you'll need to do something to implement 
    ///       the actual sending!
    /// 
    pub fn send(&mut self, sender: &crossbeam_channel::Sender<ProtocolMessage>, pm: ProtocolMessage) -> bool {
        let x: f64 = random();
        let mut result: bool = true;
        if x < self.op_success_prob {
            // TODO: implement actual send
            sender.send(pm);
        } else {

            // don't send anything!
            // (simulates failure)
            result = false;
        }
        return result;
    }

    /// 
    /// recv_request()
    /// receive a message from a client
    /// to start off the protocol.
    /// 
    pub fn recv_request(&mut self) -> Option<ProtocolMessage> {
        let mut result = Option::None;
        assert!(self.state == CoordinatorState::Quiescent);
        trace!("coordinator::recv_request...");
        // TODO: write me!
        result = Option::from(ProtocolMessage::generate(ClientRequest, 0, String::from("client_0"), 0));
        trace!("leaving coordinator::recv_request");
        result
    }

    ///
    /// report_status()
    /// report the abort/commit/unknown status (aggregate) of all 
    /// transaction requests made by this coordinator before exiting. 
    /// 
    pub fn report_status(&mut self) {
        let successful_ops: usize = 0; // TODO!
        let failed_ops: usize = 0; // TODO!
        let unknown_ops: usize = 0; // TODO! 
        println!("coordinator:\tC:{}\tA:{}\tU:{}", successful_ops, failed_ops, unknown_ops);
    }

    ///
    /// protocol()
    /// Implements the coordinator side of the 2PC protocol
    /// HINT: if the simulation ends early, don't keep handling requests!
    /// HINT: wait for some kind of exit signal before returning from the protocol!
    /// 
    pub fn protocol(&mut self) {

        // TODO!
        while self.running.load(Ordering::Relaxed) {
            trace!(" Coordinator Bool : {:?}", self.running.load(Ordering::Relaxed));
            let client_request = self.recv_request();
            let mut commited = 0;
            let mut aborted = 0;
            let mut unknown = 0;

            let mut i = 0;
            let participantsChannels = self.participantsChannels.clone();
            /// PHASE: 1
            for participantChannel in participantsChannels {
                debug!("Coordinator:: Sending Request to Participant {}", i);
                let client_request_participant = client_request.clone().unwrap();
                self.send(&participantChannel.0, client_request_participant);
                // participantChannel.0.send(client_request_participant);
                let msg = participantChannel.1.recv().expect("Error");
                debug!("Coordinator:: Reading Response {:?}", msg);
                match msg.mtype {
                    ParticipantVoteCommit => commited += 1,
                    ParticipantVoteAbort => aborted += 1,
                    _ => unknown += 1
                }
                i = i + 1;
            }

            /// PHASE: 2
            // Someone voted abort,  send abort to all participants
            if aborted > 0 {
                debug!("Someone voted abort,  send abort to all participants");
                let participantsChannels = self.participantsChannels.clone();
                for participantChannel in participantsChannels {
                    self.send(&participantChannel.0, ProtocolMessage::generate(CoordinatorAbort, 0, String::from("client_0"), 0));
                    debug!("Coordinator:: Sending Abort to Participant {}", 0);
                }
            } else {
                //All vote commit, send commit to all participants
                debug!("All vote commit, send commit to all participants");
                let participantsChannels = self.participantsChannels.clone();
                for participantChannel in participantsChannels {
                    self.send(&participantChannel.0, ProtocolMessage::generate(CoordinatorCommit, 0, String::from("client_0"), 0));
                    debug!("Coordinator:: Sending Commit to Participant {}", 0);

                }
            }
        }
        self.report_status();

        info!("Coordinator::Shutting Down");
    }
}
